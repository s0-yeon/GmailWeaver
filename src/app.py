# src/app.py

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

# 환경변수 로드
load_dotenv("src/parquet/.env") # src/parquet/.env를 사용하는 이유: GraphRAG 설정(settings.yaml)과 API 키가 같은 디렉터리에 위치하기 때문

# Flask 앱 초기화
app = Flask(__name__)   # Flask 앱 객체 생성. 해당 파일이 서버의 메인 애플리케이션이라는 의미
CORS(app)   # Cross-Origin Resource Sharing 허용 (다른 환경에서 이 서버의 API를 호출할 수 있도록)

# 한글 출력 시 깨지거나 에러 나는 것 방지 (utf-8 인코딩 및 대체 문자 처리)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# 경로 상수 정의
MAIL_DIR = "src/parquet/input"  # 업로드된 메일 텍스트 파일들을 저장할 폴더
MAIL_LATEST_PATH = os.path.join(MAIL_DIR, "mail_latest.txt")    # 최신 메일 스냅샷 파일의 고정 경로
GRAPH_BUILD_SCRIPT = "src/mail2json.py"     # 메일 텍스트 → 그래프 JSON 변환 스크립트 경로
GRAPH_JSON_PATH = "src/json/graphml_data.json"  # mail2json.py가 생성하는 그래프 JSON 결과 파일의 경로
GRAPHRAG_ROOT = "./src/parquet"     # GraphRAG 작업 루트 디렉터리

# 메모리 기반 Job 저장소
# 서버 재시작 시 모든 Job 정보 소멸 (영속성 없음)
# 멀티 워커(gunicorn -w 2 이상) 환경에서는 워커 간 공유 불가 → 운영 시 Redis 등 외부 저장소로 대체 필요
# 현재 Job 청소(cleanup) 로직 없음. 장시간 운영 시 메모리 누수 가능성
_jobs = {}

# 유틸 함수: GraphRAG CLI 실행
def _run_graphrag(message, resMethod, resType):
    def decode_output(b: bytes) -> str:
        # subprocess 결과(bytes)를 문자열로 디코딩
        # Windows 환경에서 GraphRAG가 cp949/euc-kr로 출력할 수 있으므로 UTF-8 → CP949 → EUC-KR 순으로 시도
        # 모두 실패하면 UTF-8로 강제 변환 (손실 허용)
        if not b:
            return ""
        for enc in ("utf-8", "cp949", "euc-kr"):
            try:
                return b.decode(enc)
            except UnicodeDecodeError:
                pass
        return b.decode("utf-8", errors="replace")

    # GraphRAG CLI 명령어 구성
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
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        env = os.environ.copy(),     # env=os.environ.copy(): 현재 프로세스의 환경변수 상속
        text = False    # text=False: stdout/stderr를 bytes로 받음 (직접 디코딩하기 위해)
    )
    print(f'execution_time : {time.time() - start_time}')

    stdout_text = decode_output(result.stdout)
    stderr_text = decode_output(result.stderr)

    # CLI 오류 (API 키 없음, 인덱스 없음 등)
    if result.returncode != 0:
        raise RuntimeError(stderr_text or stdout_text or 'GraphRAG 실행 오류')

    print(stdout_text)

    # GraphRAG 출력 형식에서 실제 답변 부분만 추출
    match = re.search(r'SUCCESS: (?:Local|Global) Search Response:\s*(.*)', stdout_text, re.DOTALL)
    answer = match.group(1).strip() if match else stdout_text.strip()

    # GraphRAG가 삽입하는 출처 태그 제거
    answer = re.sub(r'\[Data:.*?\]|\[데이터:.*?\]', '', answer)     # 예: "[Data: Sources (1, 2)]", "[데이터: 보고서 (3)]"

    # 마크다운 강조를 평문으로 처리
    answer = re.sub(r'\*+|#+', '', answer)

    answer = answer.strip()
    print(answer)
    return answer.strip()

# 유틸 함수: 텍스트 → 캘린더 JSON 변환
def _convert_to_calendar_json(text):
    # 자연어 텍스트에서 일정 정보를 추출하여 캘린더 이벤트 JSON으로 변환
    # OpenAI chat completions API를 직접 호출 (GraphRAG 우회, 빠른 응답)
    client = openai.OpenAI(api_key = os.environ.get("GRAPHRAG_API_KEY"))
    try:
        response = client.chat.completions.create(
            model = "gpt-4o-mini",  # gpt-4o-mini 사용: 캘린더 추출은 단순 구조화 작업이므로 저비용 모델로 충분
            response_format = {"type": "json_object"},  # JSON Mode 활성화
            messages = [
                {
                    "role": "system",
                    "content": (
                        "날짜/시간/일정 정보를 추출해서 반드시 JSON으로만 응답해. "
                        "형식: {\"events\": [{\"title\": \"제목\", \"startTime\": \"2026-02-26T09:00:00\", "
                        "\"endTime\": \"2026-02-26T10:00:00\", \"description\": \"\"}]} "
                        "일정 없으면 {\"events\": []}"
                    )
                },
                {   
                    "role": "user",
                    "content": text
                }
            ]
        )
        return json.loads(response.choices[0].message.content)
    
    except Exception as e:
        # OpenAI API 실패 시 빈 이벤트 반환 (서버 오류 전파 방지)
        print(f"[calendar convert error] {e}")
        return { "events": []}
    
# 엔드포인트: POST /extract-calendar
@app.route('/extract-calendar', methods=['POST'])
def extract_calendar():     # 이메일 제목 + 본문에서 일정 이벤트를 추출하여 반환
    data = request.json or {}
    subject = data.get('subject', '')
    body = data.get('body', '')
    result = _convert_to_calendar_json(f"제목: {subject}\n\n{body}")    # 제목과 본문을 합쳐 컨텍스트 제공
    return jsonify(result)

# 엔드포인트: POST /run-query-async
@app.route('/run-query-async', methods=['POST'])    # GraphRAG 쿼리를 백그라운드 스레드에서 비동기 실행하고 Job ID를 즉시 반환
def run_query_async():
    message = request.json.get('message', '')
    resMethod = request.json.get('resMethod', 'local')
    resType = request.json.get('resType', 'text')

    if not str(message).strip():
        return jsonify({'error': 'message가 비어있습니다.'}), 400
    
    # uuid4: 랜덤 UUID 생성. [:8]로 앞 8자리만 사용 (충돌 가능성 낮고 가독성 좋음)
    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status": "pending", "result": None, "resType": resType}

    def _worker():  # 백그라운드 스레드에서 실행되는 실제 작업 함수
        try:
            # 한국어 응답 강제 (GraphRAG 기본 응답이 영어일 경우 대비)
            full_message = message + " 영어 말고 한국어로 답변해줘."
            answer = _run_graphrag(full_message, resMethod, resType)

            if resType.lower() == "calendar":
                # 캘린더 타입: GraphRAG 텍스트 답변을 다시 OpenAI로 구조화
                result = json.dumps(_convert_to_calendar_json(answer), ensure_ascii=False)
            else:
                result = answer

            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["result"] = result

        except Exception as e:
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["result"] = str(e)

    # daemon=True: 메인 프로세스 종료 시 스레드도 함께 종료
    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"jobId": job_id})

# 엔드포인트: GET /job-status/<job_id>
@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):     # 비동기 Job의 현재 상태와 결과를 반환
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404

    if job["status"] == "done" and job["resType"].lower() == "calendar":
        try:
            # 캘린더 결과는 JSON 문자열로 저장되어 있으므로 파싱 후 반환
            return jsonify({"status": "done", "data": json.loads(job["result"])})
        except Exception:
            return jsonify({"status": "done", "data": {"events": []}})

    # text 타입: result 필드에 문자열 그대로 반환
    return jsonify({"status": job["status"], "result": job["result"] or ""})

# 엔드포인트: POST /run-query  (동기 버전, 디버깅/단순 클라이언트용)
@app.route('/run-query', methods=['POST'])
def run_query():    # GraphRAG 쿼리를 동기 방식으로 실행하고 결과를 즉시 반환
    message = request.json.get('message', '')
    resMethod = request.json.get('resMethod', 'local')
    resType = request.json.get('resType', 'text')

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

# 엔드포인트: POST /upload
@app.route("/upload", methods=["POST"])
def upload():   # Gmail 메일 데이터를 수신하여 저장하고, 그래프 인덱스를 재구축
    data = request.json or {}

    filename = data.get("filename") or f"mail_{int(time.time())}.txt"
    content  = data.get("content") or ""

    print("[UPLOAD] received filename:", filename)
    print("[UPLOAD] content length:", len(content))
    print("[UPLOAD] cwd:", os.getcwd())

    # 1단계: 메일 파일 저장
    os.makedirs(MAIL_DIR, exist_ok=True)
    file_path = os.path.join(MAIL_DIR, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    # mail_latest.txt: 항상 최신 메일 내용 유지 (덮어쓰기)
    latest_dir = os.path.dirname(MAIL_LATEST_PATH)
    if latest_dir:
        os.makedirs(latest_dir, exist_ok=True)

    with open(MAIL_LATEST_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    # 2, 3단계: 그래프 빌드 및 GraphRAG 인덱싱
    try:
        graph_dir = os.path.dirname(GRAPH_JSON_PATH)
        if graph_dir:
            os.makedirs(graph_dir, exist_ok=True)

        print("[UPLOAD] building graph... script:", GRAPH_BUILD_SCRIPT)

        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["RICH_DISABLE"] = "1"

        # 2단계: mail2json.py 실행
        r = subprocess.run(
            [sys.executable, "-X", "utf8", GRAPH_BUILD_SCRIPT],
            check=True,           # returncode != 0이면 CalledProcessError 발생
            stdout=sys.stdout,    # 자식 프로세스 출력을 현재 서버 콘솔에 직접 출력
            stderr=sys.stderr,
            env=env,
        )
        if r.stdout: print("[UPLOAD] graph build stdout:\n", r.stdout)
        if r.stderr: print("[UPLOAD] graph build stderr:\n", r.stderr)

        # 3단계: graphrag index 실행
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
        # check=True에 의해 발생. returncode로 어느 단계에서 실패했는지 확인 가능
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

# 엔드포인트: GET /graph-data
@app.route("/graph-data", methods=["GET"])
def graph_data():   # mail2json.py가 생성한 그래프 시각화 데이터를 반환
    if not os.path.exists(GRAPH_JSON_PATH):
        return jsonify({"nodes": [], "edges": [], "error": "graph json not found"}), 200
    with open(GRAPH_JSON_PATH, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))

# 서버 진입점
if __name__ == '__main__':
    # host='0.0.0.0': 모든 네트워크 인터페이스에서 수신 (localhost 외부 접근 허용)
    # port=80: 표준 HTTP 포트. Linux에서는 root 권한 필요 (또는 포트포워딩 사용)
    # debug=False: 운영 환경 설정. True로 바꾸면 코드 변경 시 자동 재시작, 에러 상세 표시
    app.run(host='0.0.0.0', port=80, debug=False)