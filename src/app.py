import os
import re
import subprocess
import time
import sys
import json
from flask import Flask, request, jsonify, send_from_directory
# jsonify는 파이썬 객체(dict, list 등)를
#HTTP 응답으로 쓸 수 있는 “JSON 형식 + 헤더”로 자동 변환해주는 Flask 도구
from flask_cors import CORS
# from gmail import getFirstMail  

# 한글 출력 시 깨지거나 에러 나는 것 방지
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace") 

app = Flask(__name__);
CORS(app)

MAIL_DIR = "src/parquet/input"
MAIL_LATEST_PATH = os.path.join(MAIL_DIR, "mail_latest.txt")

GRAPH_JSON_PATH = "src/json/graphml_data.json"   # 최종으로 쓰고 싶은 그래프 JSON 경로
GRAPH_BUILD_SCRIPT = "src/mail2json.py"           # 그래프 JSON 만드는 스크립트 파일명(너 프로젝트에 있는 걸로)

# ===== graphrag 쿼리 실행 =====
@app.route('/run-query', methods=['POST'])
def run_query():
    message = request.json.get('message', '')
    resMethod = request.json.get('resMethod', '')
    resType = request.json.get('resType', '')

    print(f'message: {message}')
    print(f'resMethod: {resMethod}')
    print(f'resType: {resType}')

    if not str(message).strip():
        return jsonify({'error': 'message가 비어있습니다.'}), 400

    message += " 영어 말고 한국어로 답변해줘."

    # Python 실행 환경에 UTF-8 인코딩 적용
    python_command = [
        'graphrag',
        'query',
        '--root',
        './src/parquet',
        '--response-type',
        resType,
        '--method',
        resMethod,
        '--query',
        message
    ]

    def decode_output(b: bytes) -> str:
        """stdout/stderr 바이트를 안전하게 문자열로 변환"""
        if not b:
            return ""
        for enc in ("utf-8", "cp949", "euc-kr"):
            try:
                return b.decode(enc)
            except UnicodeDecodeError:
                pass
        return b.decode("utf-8", errors="replace")
    
    # 명령어 실행 전에 시간 측정 시작
    start_time = time.time()  # 시작 시간 기록

    # subprocess.run을 사용하여 명령어 실행
    result = subprocess.run(
        python_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),  # Windows에선 LANG이 큰 의미 없음
        text=False
    )
    
    # 명령어 실행 후 시간 측정
    end_time = time.time()  # 종료 시간 기록
    execution_time = end_time - start_time  # 실행 시간 계산
    print(f'execution_time : {execution_time}')

    stdout_text = decode_output(result.stdout)
    stderr_text = decode_output(result.stderr)

    if result.returncode != 0:
        print(f'exec error: {stderr_text or stdout_text}')
        return jsonify({'error': stderr_text or stdout_text or 'Error occurred during execution'}), 500

    print(stdout_text)

    # 정규 표현식으로 [Data: {내용}], **.. **, # 부분을 제거
    answer = re.sub(r'.*SUCCESS: (Local|Global) Search Response:\s*', '', stdout_text, flags=re.DOTALL)  # SUCCESS 이후 내용만 남기기
    answer = re.sub(r'\[Data:.*?\]\s*|\[데이터:.*?\]\s*|\*.*?\*\s*|#', '', answer)  # [Data: ...] 및 *...* 제거
    print(answer)
  
    if resType.lower() in ("json", "json-only", "calendar-json"):
        return jsonify({'result': answer})
    # #test용  answer
    # answer = "hi!!!!!!!"

    #global recordText
    #recordText = answer
    #text_to_speech(answer)
    return jsonify({'result': answer})


# ===== gmail 데이터 플라스크 서버로 전송 =====
@app.route("/upload", methods=["POST"])
def upload():
    data = request.json or {}

    # 안전하게 꺼내기
    filename = data.get("filename") or f"mail_{int(time.time())}.txt"
    content = data.get("content") or ""

    print("[UPLOAD] received filename:", filename)
    print("[UPLOAD] content length:", len(content))
    print("[UPLOAD] cwd:", os.getcwd())
    
    # ✅ 저장 디렉토리/경로 확정
    os.makedirs(MAIL_DIR, exist_ok=True)
    file_path = os.path.join(MAIL_DIR, filename)

    # 1) 원본 filename으로 저장 (날짜로 저장)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    # 2) 최신본 고정 파일명으로 저장 (그래프 생성 시 이 파일만 읽게 하면 안정적) 1번이랑 2번 중 하나만 해도 되는데 혹시 몰라서 둘 다 해둠.
    latest_dir = os.path.dirname(MAIL_LATEST_PATH)
    if latest_dir:
        os.makedirs(latest_dir, exist_ok=True)

    with open(MAIL_LATEST_PATH, "w", encoding="utf-8") as f:
        f.write(content)

        # 3) 그래프 JSON 생성 스크립트 실행
    try:
        graph_dir = os.path.dirname(GRAPH_JSON_PATH)
        if graph_dir:
            os.makedirs(graph_dir, exist_ok=True)

        print("[UPLOAD] building graph... script:", GRAPH_BUILD_SCRIPT)

        # ✅ stdout/stderr 캡처해서 원인 확인 (python 대신 sys.executable 추천)
        r = subprocess.run(
            [sys.executable, GRAPH_BUILD_SCRIPT],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        if r.stdout:
            print("[UPLOAD] graph build stdout:\n", r.stdout)
        if r.stderr:
            print("[UPLOAD] graph build stderr:\n", r.stderr)

    except subprocess.CalledProcessError as e:
        out = (e.stdout or "").strip()
        err = (e.stderr or "").strip()
        print("[UPLOAD] graph build failed stdout:\n", out)
        print("[UPLOAD] graph build failed stderr:\n", err)

        return jsonify({
            "ok": False,
            "error": "graph build failed",
            "returncode": e.returncode,
            "stdout": out[-4000:],   # 너무 길면 뒤쪽만
            "stderr": err[-4000:],
        }), 500

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
    app.run(host='0.0.0.0', port=80, debug=True)
    # app.run(host='0.0.0.0', port=5000, debug=True) # local 환경설정