import os
import re
import subprocess
import time
import sys
from flask import Flask, request, jsonify
# jsonify는 파이썬 객체(dict, list 등)를
#HTTP 응답으로 쓸 수 있는 “JSON 형식 + 헤더”로 자동 변환해주는 Flask 도구
from flask_cors import CORS
# from gmail import getFirstMail  
# 

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace") 
app = Flask(__name__);
CORS(app)

@app.route('/mails', methods=['GET'])
def get_mail(): 
    print("HIT /mails")
    request_data = request.args
    title = request_data.get('title', 'No Title Provided')
    print(title)
    return jsonify({
        "ok": True,
        "message": "Server received your title!",
        "echoedSubject": title,
        })

recordText=""

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
  
    # #test용  answer
    # answer = "hi!!!!!!!"

    #global recordText
    #recordText = answer
    #text_to_speech(answer)
    return jsonify({'result': answer})

@app.route("/upload", methods=["POST"])
def upload():
    data = request.json
    with open(f"src/parquet/input/{data['filename']}", "w", encoding="utf-8") as f:
        f.write(data["content"])
    return {"ok": True}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
    # app.run(host='0.0.0.0', port=5000, debug=True) # local 환경설정