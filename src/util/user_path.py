import re,os
import json
from config.settings import GRAPHRAG_SETTINGS_DIR, GRAPHRAG_PROMPTS_DIR
import shutil

ACCOUNT_META_FILENAME = "account.json"

_MAX_MAILS_CONFIG = {
    "ddyr1554@gmail.com": 200,
    "inhist44@gmail.com": 1000,
    "cjiyou1090@gmail.com": 1500,
    "csi10186@gmail.com": 500,
    "03yeah03@gmail.com": 300,
}

def _gmail_to_dir_name(user_id: str) -> str:
    s = user_id.strip().lower()
    s = s.replace("@", "_at_")
    s = s.replace(".", "_")
    s = s.replace("+", "_plus_")
    s = re.sub(r"[^a-z0-9_]", "_", s)
    return s

class UserPaths:
    def __init__(self, base_dir: str, user_id: str):
        self.BASE_DIR = base_dir
        self.USER_ID = user_id

        dir_name = _gmail_to_dir_name(user_id)

        self.USER_ROOT = os.path.join(base_dir, "user_data", dir_name)
        self.GRAPHRAG_ROOT = os.path.join(self.USER_ROOT, "parquet")

        self.USER_GRAPH_SETTINGS_PATH = os.path.join(self.GRAPHRAG_ROOT, "settings.yaml")
        self.USER_GRAPH_PROMPTS_PATH = os.path.join(self.GRAPHRAG_ROOT, "prompts")

        self.GRAPH_JSON_PATH = os.path.join(self.USER_ROOT, "json", "graphml_data.json")
        self.GRAPH_BUILD_SCRIPT = os.path.join(base_dir, "src", "parquet2json.py")

        self.MAIL_DIR = os.path.join(self.GRAPHRAG_ROOT, "input")
        self.MAIL_LATEST_PATH = os.path.join(self.MAIL_DIR, "mail_latest.txt")
        self.ATTACHMENT_DIR = os.path.join(self.MAIL_DIR, "attachments")

        self.PARQUET_DIR = os.path.join(self.USER_ROOT, "parquet", "output") # output 폴더: parquet들 저장
        self.ENTITIES_PATH = os.path.join(self.PARQUET_DIR, "entities.parquet") # 노드 데이터: 엔티티 목록
        self.RELATIONSHIPS_PATH = os.path.join(self.PARQUET_DIR, "relationships.parquet") # 엣지 데이터: 엔티티 간 관계
        self.COMMUNITIES_PATH = os.path.join(self.PARQUET_DIR, "communities.parquet") # 커뮤니티 데이터: 군집화한 노드 그룹 정보
        self.MAIL_STATICS_PATH = os.path.join(self.PARQUET_DIR, "statics")
        self.MAIL_CONTACTS_PATH = os.path.join(self.MAIL_STATICS_PATH, "mail_contact_stats.json")
        self.MAIL_KEYWORDS_PATH  = os.path.join(self.MAIL_STATICS_PATH, "mail_keyword_stats.json")
        self.MAIL_SUMMARIES_PATH = os.path.join(self.MAIL_STATICS_PATH, "mail_summaries.json")
        self.MAIL_PHOTOS_PATH    = os.path.join(self.MAIL_STATICS_PATH, "contact_photos.json")
        self.MAIL_AVATARS_PATH  = os.path.join(self.MAIL_STATICS_PATH, "person_avatars.json")
        self.AVATAR_IMAGES_DIR  = os.path.join(self.MAIL_STATICS_PATH, "avatars")
        self.MAIL_MESSAGE_CACHE_PATH = os.path.join(self.MAIL_STATICS_PATH, "mail_message_cache.json")
        self.UPDATE_DIR = os.path.join(self.GRAPHRAG_ROOT, "update_output")
        self.MAX_MAILS = _MAX_MAILS_CONFIG.get(user_id, None)

        self.ACCOUNT_META_PATH = os.path.join(self.USER_ROOT, ACCOUNT_META_FILENAME)
        _ensure_account_meta(self)


# 계정 폴더가 이미 존재하는 경우에만 원본 user_id를 메타 파일로 남겨서
# 나중에 폴더명(디렉터리 sanitize로 인해 원본 문자열이 손실됨)만으로도
# 실제 계정 목록을 복원할 수 있게 한다. (/accounts 엔드포인트에서 사용)
def _ensure_account_meta(paths: "UserPaths"):
    if not os.path.isdir(paths.USER_ROOT) or os.path.exists(paths.ACCOUNT_META_PATH):
        return
    try:
        with open(paths.ACCOUNT_META_PATH, "w", encoding="utf-8") as f:
            json.dump({"user_id": paths.USER_ID}, f, ensure_ascii=False)
    except OSError:
        pass



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





