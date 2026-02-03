from flask import Flask, render_template, jsonify, request
import json
import os
import time
import subprocess
import re
import sys
from flask_cors import CORS

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace") 
app = Flask(__name__)
CORS(app)

# JSON 파일 경로 설정
JSON_FILE_PATH = './json/graph.json'  # GraphRAG 출력 경로에 맞게 수정

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')

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
    """GraphRAG 쿼리 실행 (CLI 방식)"""
    message = request.json.get('message', '')
    resMethod = request.json.get('resMethod', 'local')
    resType = request.json.get('resType', 'text')

    print(f'message: {message}')
    print(f'resMethod: {resMethod}')
    print(f'resType: {resType}')

    if not str(message).strip():
        return jsonify({'error': 'message가 비어있습니다.'}), 400

    # 한국어 답변 요청 추가
    message += " 영어 말고 한국어로 답변해줘."

   # ✅ 수정: 경로를 ./src/parquet로 변경
    root_path = os.path.join(os.getcwd(), 'parquet')

    # graphrag CLI 명령어 구성
    python_command = [
        'graphrag',
        'query',
        '--root',
        root_path,  # ✅ ./src/parquet
        '--response-type',
        resType,
        '--method',
        resMethod,
        '--query',
        message
    ]


    print(f'실행 명령어: {" ".join(python_command)}')

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
    
    # 시간 측정 시작
    start_time = time.time()

    # subprocess로 명령어 실행
    result = subprocess.run(
        python_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),
        text=False
    )
    
    # 실행 시간 계산
    end_time = time.time()
    execution_time = end_time - start_time
    print(f'execution_time : {execution_time:.2f}초')

    stdout_text = decode_output(result.stdout)
    stderr_text = decode_output(result.stderr)

    if result.returncode != 0:
        print(f'실행 오류: {stderr_text or stdout_text}')
        return jsonify({'error': stderr_text or stdout_text or 'Error occurred during execution'}), 500

    print(f'원본 응답:\n{stdout_text}')

    # 정규 표현식으로 불필요한 부분 제거
    answer = re.sub(r'.*SUCCESS: (Local|Global) Search Response:\s*', '', stdout_text, flags=re.DOTALL)
    answer = re.sub(r'\[Data:.*?\]\s*|\[데이터:.*?\]\s*|\*.*?\*\s*|#', '', answer)
    answer = answer.strip()
    
    print(f'정제된 답변:\n{answer}')

    # ✅ Google Apps Script와 호환되는 응답 형식
    return jsonify({
        'status': 'success',
        'result': answer,  # 기존 코드 호환
        'response': answer,  # 새 코드 호환
        'execution_time': f'{execution_time:.2f}초'
    })



@app.route('/api/graph-data')
def graph_data():
    """GraphRAG JSON 데이터 로드 및 변환"""
    try:
        # JSON 파일 존재 확인
        if not os.path.exists(JSON_FILE_PATH):
            return jsonify({
                "error": f"JSON 파일을 찾을 수 없습니다: {JSON_FILE_PATH}",
                "nodes": [],
                "edges": []
            }), 404
        
        # JSON 파일 읽기
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 데이터 검증 및 변환
        processed_data = process_graph_data(data)
        
        return jsonify(processed_data)
        
    except json.JSONDecodeError as e:
        return jsonify({
            "error": f"JSON 파싱 오류: {str(e)}",
            "nodes": [],
            "edges": []
        }), 400
    except Exception as e:
        return jsonify({
            "error": f"서버 오류: {str(e)}",
            "nodes": [],
            "edges": []
        }), 500


def process_graph_data(data):
    """
    GraphRAG 데이터를 D3.js 시각화에 맞게 변환
    """
    if not data or 'nodes' not in data or 'edges' not in data:
        return {"nodes": [], "edges": []}
    
    # 노드 처리
    processed_nodes = []
    node_ids = set()
    
    for node in data['nodes']:
        node_id = node.get('id', '')
        if not node_id:
            continue
        
        node_ids.add(node_id)
        
        # 연결 수 계산 (degree 계산)
        degree = sum(1 for edge in data['edges'] 
                    if edge['source'] == node_id or edge['target'] == node_id)
        
        processed_nodes.append({
            'id': node_id,
            'description': node.get('description') or f"Entity: {node_id}",
            'degree': max(degree, 1),  # 최소 1 이상
            'type': node.get('entity_type') or 'default',
            'weight': node.get('weight') or 1,
            'cluster': node.get('cluster') or 0
        })
    
    # 엣지 처리 (존재하는 노드만)
    processed_edges = []
    for edge in data['edges']:
        source = edge.get('source', '')
        target = edge.get('target', '')
        
        # source와 target이 모두 존재하는 노드인지 확인
        if source in node_ids and target in node_ids:
            processed_edges.append({
                'source': source,
                'target': target,
                'relationship': edge.get('description') or '연관',
                'weight': edge.get('weight') or 1
            })
    
    print(f"✅ 처리된 데이터: {len(processed_nodes)} 노드, {len(processed_edges)} 엣지")
    
    return {
        'nodes': processed_nodes,
        'edges': processed_edges
    }

@app.route("/upload", methods=["POST"])
def upload():
    data = request.json
    with open(f"src/parquet/input/{data['filename']}", "w", encoding="utf-8") as f:
        f.write(data["content"])
    return {"ok": True}

if __name__ == '__main__':
    print(f"📂 JSON 파일 경로: {JSON_FILE_PATH}")
    print(f"📂 파일 존재 여부: {os.path.exists(JSON_FILE_PATH)}")
    app.run(debug=True, port=8000)
