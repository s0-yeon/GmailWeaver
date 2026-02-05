import os
import json
import networkx as nx
from flask import Flask, request, jsonify
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

# 경로 설정
BASE_DIR = Path(__file__).resolve().parent.parent
JSON_PATH = BASE_DIR / "json" / "graphml_data.json"

@app.route('/run-query', methods=['POST'])
def run_query():
    data = request.json
    user_msg = data.get('message', '')
    mail_id = data.get('mail_id', '')
    action = data.get('action', '') # 요청에 따른 액션 구분

    # 1. 삭제 액션 처리
    if action == 'delete':
        return jsonify({
            "status": "success",
            "message": f"메일 ID {mail_id}가 삭제되었습니다.",
            "evidence_id": mail_id
        })

    # 2. 근거 메일 보기 및 기본 분석 로직 (초기 버전)
    # 별도의 LLM 없이 입력된 메시지를 기반으로 결과 반환
    return jsonify({
        "result": f"메일 내용 요약: {user_msg[:50]}...",
        "evidence_id": mail_id,
        "status": "completed"
    })

if __name__ == '__main__':
    # 시스템 파이썬에서 포트 80으로 실행
    app.run(host='0.0.0.0', port=80)