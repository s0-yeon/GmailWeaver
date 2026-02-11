import os
import re
import json
import glob
from datetime import datetime, timezone, timedelta

# =========================
# Paths
# =========================
INPUT_DIR = "./parquet/input"
MAIL_GLOB = os.path.join(INPUT_DIR, "gmail_ALL_inbox_sent_*.txt")
JSON_OUT_PATH = "./json/graphml_data.json"

def get_latest_mail_txt_path():
    candidates = glob.glob(MAIL_GLOB)
    if not candidates:
        raise FileNotFoundError(f"No mail txt files found in {INPUT_DIR}")
    return max(candidates, key=os.path.getmtime)

MAIL_TXT_PATH = get_latest_mail_txt_path()
print(f"[INFO] Using mail file: {MAIL_TXT_PATH}")

# =========================
# Config
# =========================
ALLOWED_TYPES = {
    "EMAIL", "EMAIL_ADDRESS", "DATE",
    "PERSON", "ORGANIZATION", "SERVICE",
    "SUBJECT", "FILE", "FILE_TYPE"
}

ALLOWED_REL_TYPES = {
    "OWNS", "FROM_ADDRESS", "TO_ADDRESS", "CC_ADDRESS", "SENT_AT",
    "HAS_SUBJECT", "CONTAINS", "HAS_FILE_TYPE",
    "SENDS_TO", "RELATES_TO"
}

MAIL_BLOCK_SEP = "============================================================"

# =========================
# Helpers
# =========================
def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def upper_email(s: str) -> str:
    return (s or "").strip().upper()

def safe_add_node(nodes_by_id, node_id, node_type, description="", properties=None):
    if node_type not in ALLOWED_TYPES or not node_id:
        return
    if node_id not in nodes_by_id:
        nodes_by_id[node_id] = {
            "id": node_id,
            "type": node_type,
            "description": description or "",
        }
        if properties is not None:
            nodes_by_id[node_id]["properties"] = properties

def _fallback_edge_desc(source, target, rel_type):
    if rel_type == "SENDS_TO":
        return f"{source} sent an email to {target}."
    if rel_type == "RELATES_TO":
        return f'{source} is related to "{target}".'
    return f"{source} -> {target} ({rel_type})"

def add_edge(edges, edge_key_set, source, target, rel_type, description=""):
    if not source or not target or rel_type not in ALLOWED_REL_TYPES:
        return

    desc = description.strip() if description else _fallback_edge_desc(source, target, rel_type)
    key = (source, target, rel_type, desc)
    if key in edge_key_set:
        return
    edge_key_set.add(key)

    edges.append({
        "id": f"{source}__{rel_type}__{target}",
        "source": source,
        "target": target,
        "type": rel_type,
        "relationship": rel_type,
        "description": desc,
        "label": desc,
        "title": desc,
        "tooltip": desc,
    })

def parse_name_and_email(raw):
    raw = (raw or "").strip()
    if not raw:
        return None, None
    m = re.search(r'<([^>]+)>', raw)
    if m:
        name = raw[:m.start()].strip().strip('"')
        return name or None, m.group(1).strip()
    if "@" in raw:
        return None, raw
    return raw, None

def classify_sender_name(name):
    if not name:
        return None, None
    org_markers = ["회사", "주식회사", "INC", "LLC", "CORP", "TEAM"]
    if any(k in name.upper() for k in org_markers):
        return name, "ORGANIZATION"
    return name, "PERSON"

def normalize_date_to_iso(date_raw):
    if not date_raw:
        return None
    tz = re.search(r'GMT([+-]\d{4})', date_raw)
    tzinfo = None
    if tz:
        sign = 1 if tz.group(1)[0] == "+" else -1
        hh = int(tz.group(1)[1:3])
        mm = int(tz.group(1)[3:5])
        tzinfo = timezone(sign * timedelta(hours=hh, minutes=mm))
    m = re.search(r'([A-Za-z]{3}\s+[A-Za-z]{3}\s+\d+\s+\d{4}\s+\d{2}:\d{2}:\d{2})', date_raw)
    if not m:
        return None
    dt = datetime.strptime(m.group(1), "%a %b %d %Y %H:%M:%S")
    return dt.replace(tzinfo=tzinfo).isoformat() if tzinfo else dt.isoformat()

def extract_section(text, header):
    pattern = re.escape(header) + r"\s*\n"
    m = re.search(pattern, text)
    if not m:
        return ""
    start = m.end()
    for h in ["[본문]", "[첨부파일 정보]"]:
        if h != header:
            m2 = re.search(re.escape(h), text[start:])
            if m2:
                return text[start:start + m2.start()].strip()
    return text[start:].strip()

def parse_attachments(block):
    if not block or "첨부파일: 없음" in block:
        return [], ""
    parsed = []
    for ln in block.splitlines():
        ln = ln.strip()
        m = re.match(r'\d+\.\s*(.+?)\s*\|\s*([^|]+?)\s*\|', ln)
        if m:
            parsed.append({"filename": m.group(1), "mime_type": m.group(2)})
    return parsed, block

def split_mail_blocks(text):
    return [c for c in text.split(MAIL_BLOCK_SEP) if "ID:" in c]

def parse_mail_block(block):
    def grab(p):
        m = re.search(rf'^{re.escape(p)}\s*(.*)$', block, re.MULTILINE)
        return m.group(1).strip() if m else ""

    props = {
        "mail_id": grab("ID:"),
        "direction": grab("구분:"),
        "subject": grab("제목:") or "(제목 없음)",
        "from_raw": grab("보낸 사람:"),
        "to_raw": grab("받는 사람:"),
        "cc_raw": grab("참조(CC):"),
        "date_raw": grab("날짜:"),
        "body_raw": extract_section(block, "[본문]")
    }

    props["datetime_iso"] = normalize_date_to_iso(props["date_raw"])
    atts, _ = parse_attachments(extract_section(block, "[첨부파일 정보]"))
    props["attachments"] = atts
    return props

# =========================
# Main
# =========================
with open(MAIL_TXT_PATH, "r", encoding="utf-8") as f:
    all_text = f.read()

nodes_by_id = {}
edges = []
edge_key_set = set()

blocks = split_mail_blocks(all_text)

for b in blocks:
    mail = parse_mail_block(b)
    email_node = f"메일 {mail['mail_id']}"

    safe_add_node(nodes_by_id, email_node, "EMAIL", "", mail)

    subject = mail["subject"]
    safe_add_node(nodes_by_id, subject, "SUBJECT", f'Email subject "{subject}".')
    add_edge(edges, edge_key_set, email_node, subject, "HAS_SUBJECT")

    iso = mail.get("datetime_iso")
    if iso:
        date_node = f"DATE_{iso}"
        safe_add_node(nodes_by_id, date_node, "DATE", f"Sent at {iso}.")
        add_edge(edges, edge_key_set, email_node, date_node, "SENT_AT")

    from_name, from_email = parse_name_and_email(mail["from_raw"])
    to_name, to_email = parse_name_and_email(mail["to_raw"])

    if from_email:
        addr = upper_email(from_email)
        safe_add_node(nodes_by_id, addr, "EMAIL_ADDRESS", "")
        add_edge(edges, edge_key_set, email_node, addr, "FROM_ADDRESS")

    if to_email:
        addr = upper_email(to_email)
        safe_add_node(nodes_by_id, addr, "EMAIL_ADDRESS", "")
        add_edge(edges, edge_key_set, email_node, addr, "TO_ADDRESS")

    sender, sender_type = classify_sender_name(from_name)
    if sender:
        safe_add_node(nodes_by_id, sender, sender_type, "")
        if to_name:
            safe_add_node(nodes_by_id, to_name, "PERSON", "")
            add_edge(edges, edge_key_set, sender, to_name, "SENDS_TO",
                     f'{sender} sent an email to {to_name} about "{subject}".')

    for att in mail["attachments"]:
        fname = att["filename"]
        mime = att.get("mime_type")
        safe_add_node(nodes_by_id, fname, "FILE", "")
        add_edge(edges, edge_key_set, email_node, fname, "CONTAINS")
        add_edge(edges, edge_key_set, subject, fname, "RELATES_TO")
        if mime:
            safe_add_node(nodes_by_id, mime, "FILE_TYPE", "")
            add_edge(edges, edge_key_set, fname, mime, "HAS_FILE_TYPE")

# =========================
# Finalize (NO FILTERING)
# =========================
all_nodes = list(nodes_by_id.values())
graph_data = {
    "nodes": all_nodes,
    "edges": edges
}

ensure_dir(JSON_OUT_PATH)
with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(graph_data, f, ensure_ascii=False, indent=2)

print(f"nodes={len(all_nodes)}, edges={len(edges)}")
