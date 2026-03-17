import os

# 경로 상수 정의
MAIL_DIR = "src/parquet/input"  # 업로드된 메일 텍스트 파일들을 저장할 폴더
MAIL_LATEST_PATH = os.path.join(MAIL_DIR, "mail_latest.txt")    # 최신 메일 스냅샷 파일의 고정 경로
GRAPH_BUILD_SCRIPT = "src/mail2json.py"     # 메일 텍스트 → 그래프 JSON 변환 스크립트 경로
GRAPH_JSON_PATH = "src/json/graphml_data.json"  # mail2json.py가 생성하는 그래프 JSON 결과 파일의 경로
GRAPHRAG_ROOT = "./src/parquet"     # GraphRAG 작업 루트 디렉터리
MAIL_BLOCK_SEP = "============================================================"     # 메일 블록 구분자
ATTACHMENT_DIR = os.path.join(MAIL_DIR, "attachments")  # 첨부 원본 파일 저장 폴더