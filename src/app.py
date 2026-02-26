import os
import re
import subprocess
import time
import sys
import json
import threading
import uuid
import openai
from dotenv import load_dotenv

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# .env 파일에서 API 키 로드
load_dotenv("src/parquet/.env")

app = Flask(__name__)
CORS(app)

# 한글 출력 시 깨지거나 에러 나는 것 방지
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ===== 상수 =====
MAIL_DIR = "src/parquet/input"
MAIL_LATEST_PATH = os.path.join(MAIL_DIR, "mail_latest.txt")
GRAPH_JSON_PATH = "src/json/graphml_data.json"
GRAPH_BUILD_SCRIPT = "src/mail2json.py"
GRAPHRAG_ROOT = "./src/parquet"

# ===== Job 저장소 (메모리) =====
_jobs = {}


# ===== 유틸: GraphRAG 실행 =====
def _run_graphrag(message, resMethod, resType):
    def decode_output(b: bytes) -> str:
        if not b:
            return ""
        for enc in ("utf-8", "cp949", "euc-kr"):
            try:
                return b.decode(enc)
            except UnicodeDecodeError:
                pass
        return b.decode("utf-8", errors="replace")

    python_command = [
        'graphrag', 'query',
        '--root', './src/parquet',
        '--response-type', resType,
        '--method', resMethod,
        '--query', message
    ]

    start_time = time.time()
    result = subprocess.run(
        python_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),
        text=False
    )
    print(f'execution_time : {time.time() - start_time}')

    stdout_text = decode_output(result.stdout)
    stderr_text = decode_output(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(stderr_text or stdout_text or 'GraphRAG 실행 오류')

    print(stdout_text)

    match = re.search(r'SUCCESS: (?:Local|Global) Search Response:\s*(.*)', stdout_text, re.DOTALL)
    answer = match.group(1).strip() if match else stdout_text.strip()
    answer = re.sub(r'\[Data:.*?\]|\[데이터:.*?\]', '', answer)
    answer = re.sub(r'\*+|#+', '', answer)
    answer = answer.strip()
    print(answer)
    return answer.strip()


# ===== 유틸: 텍스트 → 캘린더 JSON 변환 (OpenAI 직접 호출) =====
def _convert_to_calendar_json(text):
    client = openai.OpenAI(api_key=os.environ.get("GRAPHRAG_API_KEY"))
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "날짜/시간/일정 정보를 추출해서 반드시 JSON으로만 응답해. "
                        "형식: {\"events\": [{\"title\": \"제목\", \"startTime\": \"2026-02-26T09:00:00\", "
                        "\"endTime\": \"2026-02-26T10:00:00\", \"description\": \"\"}]} "
                        "일정 없으면 {\"events\": []}"
                    )
                },
                {"role": "user", "content": text}
            ]
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"[calendar convert error] {e}")
        return {"events": []}

# ===== 캘린더 추출 (OpenAI 직접, 빠름) =====
@app.route('/extract-calendar', methods=['POST'])
def extract_calendar():
    data    = request.json or {}
    subject = data.get('subject', '')
    body    = data.get('body', '')
    result  = _convert_to_calendar_json(f"제목: {subject}\n\n{body}")
    return jsonify(result)


# ===== 비동기 쿼리 실행 요청 =====
@app.route('/run-query-async', methods=['POST'])
def run_query_async():
    message   = request.json.get('message', '')
    resMethod = request.json.get('resMethod', 'local')
    resType   = request.json.get('resType', 'text')

    if not str(message).strip():
        return jsonify({'error': 'message가 비어있습니다.'}), 400

    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status": "pending", "result": None, "resType": resType}

    def _worker():
        try:
            full_message = message + " 영어 말고 한국어로 답변해줘."
            answer = _run_graphrag(full_message, resMethod, resType)
            if resType.lower() == "calendar":
                result = json.dumps(_convert_to_calendar_json(answer), ensure_ascii=False)
            else:
                result = answer
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["result"] = result
        except Exception as e:
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["result"] = str(e)

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"jobId": job_id})


# ===== Job 상태 확인 =====
@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404

    if job["status"] == "done" and job["resType"].lower() == "calendar":
        try:
            return jsonify({"status": "done", "data": json.loads(job["result"])})
        except Exception:
            return jsonify({"status": "done", "data": {"events": []}})

    return jsonify({"status": job["status"], "result": job["result"] or ""})


# ===== graphrag 쿼리 실행 (동기) =====
@app.route('/run-query', methods=['POST'])
def run_query():
    message   = request.json.get('message', '')
    resMethod = request.json.get('resMethod', 'local')
    resType   = request.json.get('resType', 'text')

    print(f'message: {message}')
    print(f'resMethod: {resMethod}')
    print(f'resType: {resType}')

    if not str(message).strip():
        return jsonify({'error': 'message가 비어있습니다.'}), 400

    message += " 영어 말고 한국어로 답변해줘."

    try:
        answer = _run_graphrag(message, resMethod, resType)
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500

    if resType.lower() == "calendar":
        return jsonify(_convert_to_calendar_json(answer))

    return jsonify({'result': answer})


# ===== gmail 데이터 플라스크 서버로 전송 =====
@app.route("/upload", methods=["POST"])
def upload():
    data = request.json or {}

    filename = data.get("filename") or f"mail_{int(time.time())}.txt"
    content  = data.get("content") or ""

    print("[UPLOAD] received filename:", filename)
    print("[UPLOAD] content length:", len(content))
    print("[UPLOAD] cwd:", os.getcwd())

    os.makedirs(MAIL_DIR, exist_ok=True)
    file_path = os.path.join(MAIL_DIR, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    latest_dir = os.path.dirname(MAIL_LATEST_PATH)
    if latest_dir:
        os.makedirs(latest_dir, exist_ok=True)

    with open(MAIL_LATEST_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    try:
        graph_dir = os.path.dirname(GRAPH_JSON_PATH)
        if graph_dir:
            os.makedirs(graph_dir, exist_ok=True)

        print("[UPLOAD] building graph... script:", GRAPH_BUILD_SCRIPT)

        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["RICH_DISABLE"] = "1"

        r = subprocess.run(
            [sys.executable, "-X", "utf8", GRAPH_BUILD_SCRIPT],
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )
        if r.stdout: print("[UPLOAD] graph build stdout:\n", r.stdout)
        if r.stderr: print("[UPLOAD] graph build stderr:\n", r.stderr)

        print("[UPLOAD] building graphrag index... root:", GRAPHRAG_ROOT)
        r2 = subprocess.run(
            [sys.executable, "-X", "utf8", "-m", "graphrag", "index", "--root", GRAPHRAG_ROOT],
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )
        if r2.stdout: print("[UPLOAD] index stdout:\n", r2.stdout)
        if r2.stderr: print("[UPLOAD] index stderr:\n", r2.stderr)

    except subprocess.CalledProcessError as e:
        print("[UPLOAD] build failed. returncode:", e.returncode)
        return jsonify({"ok": False, "error": "graph build failed", "returncode": e.returncode}), 500
    except Exception as e:
        print("[UPLOAD] unexpected error:", str(e))
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "saved_path": os.path.abspath(file_path),
        "latest_path": os.path.abspath(MAIL_LATEST_PATH),
        "content_length": len(content),
    })

@app.route("/graph-data", methods=["GET"])
def graph_data():
    if not os.path.exists(GRAPH_JSON_PATH):
        return jsonify({"nodes": [], "edges": [], "error": "graph json not found"}), 200
    with open(GRAPH_JSON_PATH, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=False)