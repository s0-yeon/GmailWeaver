import re,os
from config.settings import BASE_DIR

def gmail_to_dir_name(gmail_id: str) -> str:
    s = gmail_id.strip().lower()
    s = s.replace("@", "_at_")
    s = s.replace(".", "_")
    s = s.replace("+", "_plus_")
    s = re.sub(r"[^a-z0-9_]", "_", s)
    return s

class UserPaths:
    def __init__(self, base_dir: str, gmail_id: str):
        dir_name = gmail_to_dir_name(gmail_id)

        self.USER_ROOT = os.path.join(base_dir, "user_data", dir_name)
        self.GRAPHRAG_ROOT = os.path.join(self.USER_ROOT, "parquet")
        self.MAIL_DIR = os.path.join(self.GRAPHRAG_ROOT, "input")
        self.MAIL_LATEST_PATH = os.path.join(self.MAIL_DIR, "mail_latest.txt")
        self.ATTACHMENT_DIR = os.path.join(self.MAIL_DIR, "attachments")
        self.GRAPH_JSON_PATH = os.path.join(self.USER_ROOT, "json", "graphml_data.json")