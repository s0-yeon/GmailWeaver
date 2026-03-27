import re,os
from config.settings import GRAPHRAG_SETTINGS_DIR, GRAPHRAG_PROMPTS_DIR
import shutil

def _gmail_to_dir_name(gmail_id: str) -> str:
    s = gmail_id.strip().lower()
    s = s.replace("@", "_at_")
    s = s.replace(".", "_")
    s = s.replace("+", "_plus_")
    s = re.sub(r"[^a-z0-9_]", "_", s)
    return s

class UserPaths:
    def __init__(self, base_dir: str, gmail_id: str):
        dir_name = _gmail_to_dir_name(gmail_id)

        self.USER_ROOT = os.path.join(base_dir, "user_data", dir_name)
        self.GRAPHRAG_ROOT = os.path.join(self.USER_ROOT, "parquet")

        self.USER_GRAPH_SETTINGS_PATH = os.path.join(self.GRAPHRAG_ROOT, "settings.yaml")
        self.USER_GRAPH_PROMPTS_PATH = os.path.join(self.GRAPHRAG_ROOT, "prompts")

        self.GRAPH_JSON_PATH = os.path.join(self.USER_ROOT, "json", "graphml_data.json")
        self.GRAPH_BUILD_SCRIPT = os.path.join(base_dir, "src", "mail2json.py")

        self.MAIL_DIR = os.path.join(self.GRAPHRAG_ROOT, "input")
        self.MAIL_LATEST_PATH = os.path.join(self.MAIL_DIR, "mail_latest.txt")
        self.ATTACHMENT_DIR = os.path.join(self.MAIL_DIR, "attachments")



# 공용 settings.yaml, prompts 를 사용자 parquet 폴더에 복사
def user_graphrag_init(paths):

    # 1. parquet 폴더 보장
    os.makedirs(paths.GRAPHRAG_ROOT, exist_ok=True)

    # 2. 공용 템플릿 존재 확인
    if not os.path.exists(GRAPHRAG_SETTINGS_DIR):
        raise FileNotFoundError(
            f"[ERROR] 공용 settings.yaml 없음: {GRAPHRAG_SETTINGS_DIR}"
        )
    if not os.path.exists(GRAPHRAG_PROMPTS_DIR):
        raise FileNotFoundError(
            f"[ERROR] 공용 prompts 폴더 없음: {GRAPHRAG_PROMPTS_DIR}"
        )

    # 3. settings.yaml복사(있으면 덮어쓰기), 항상 최신 settings.yaml을 사용하기 위함
    shutil.copy2(GRAPHRAG_SETTINGS_DIR, paths.USER_GRAPH_SETTINGS_PATH)
    print(f"[INIT] settings.yaml 복사/덮어쓰기 완료 → {paths.USER_GRAPH_SETTINGS_PATH}")

    # 4. prompts 폴더 전체 복사(있으면 덮어쓰기), 항상 최신 prompts를 사용하기 위함
    shutil.copytree(
        GRAPHRAG_PROMPTS_DIR,
        paths.USER_GRAPH_PROMPTS_PATH,
        dirs_exist_ok=True  # 기존 프롬프트가 있으면 덮어쓰기
    )





