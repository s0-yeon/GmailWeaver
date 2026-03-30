import os

# 여기서 3번 올라가면 프로젝트 루트
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GRAPHRAG_SETTINGS_DIR = os.path.join(BASE_DIR,"parquet_template","settings.yaml")
GRAPHRAG_PROMPTS_DIR = os.path.join(BASE_DIR,"parquet_template", "prompts")
# config/setting.py -> src/config/setting.py

MAIL_BLOCK_SEP = "============================================================"     # 메일 블록 구분자

#아래 경로는 사용자 마다 달라지므로 재정의 필요
#GRAPH_BUILD_SCRIPT = os.path.join(BASE_DIR, "src", "parquet2json.py")     # 메일 텍스트 → 그래프 JSON 변환 스크립트 경로
#GRAPHRAG_ROOT = os.path.join(BASE_DIR, "src", "parquet")     # GraphRAG 작업 루트 디렉터리
# MAIL_DIR = os.path.join(BASE_DIR, "src", "parquet", "input")  # 업로드된 메일 텍스트 파일들을 저장할 폴더
# MAIL_LATEST_PATH = os.path.join(MAIL_DIR, "mail_latest.txt")    # 최신 메일 스냅샷 파일의 고정 경로
# ATTACHMENT_DIR = os.path.join(MAIL_DIR, "attachments")  # 첨부 원본 파일 저장 폴더
# PARQUET_DIR = os.path.join(BASE_DIR, "src", "parquet", "output") # output 폴더: parquet들 저장
# ENTITIES_PATH = os.path.join(PARQUET_DIR, "entities.parquet") # 노드 데이터: 엔티티 목록
# RELATIONSHIPS_PATH = os.path.join(PARQUET_DIR, "relationships.parquet") # 엣지 데이터: 엔티티 간 관계
# COMMUNITIES_PATH = os.path.join(PARQUET_DIR, "communities.parquet") # 커뮤니티 데이터: 군집화한 노드 그룹 정보


