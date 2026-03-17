# src/mail2json.py

import os
import re
import json
from typing import Dict, Any, Optional

# 경로 (루트에서 python src/app.py 실행 기준)
INPUT_DIR = "src/parquet/input"     
MAIL_TXT_PATH = os.path.join(INPUT_DIR, "mail_latest.txt")  # 읽을 메일 파일
JSON_OUT_PATH = "src/json/graphml_data.json"    # 출력할 JSON

HIDDEN_STRUCT_TYPES = {"EMAIL"} # 그래프 시각화에서 숨기는 노드 타입 (질의응답용)

ALLOWED_TYPES = {   # 허용된 노드 종류
    "EMAIL", # 메일 전체 (질의응답용)
    "PERSON", # 사람+메일주소
    "ORGANIZATION", # 기관 + 메일주소
    "SUBJECT", # 메일 제목
    "FILE", # 첨부파일 제목
}

ALLOWED_REL_TYPES = {   # 허용된 엣지 종류
    "SENDS_TO",
    "RELATES_TO",
}

MAIL_BLOCK_SEP = "============================================================"     # 메일 블록 구분자

# 헬퍼 함수
def ensure_dir_for_file(file_path: str):    # 출력 경로 보장
    dir_path = os.path.dirname(file_path)   # 파일명 제거하고 경로만 추출
    if dir_path:    
        os.makedirs(dir_path, exist_ok=True)    

def safe_add_node(nodes_by_id: Dict[str, dict], node_id: str, node_type: str, description: str = "", properties: Optional[Dict[str, Any]] = None):  # 저장 및 노드 추가
    if node_type not in ALLOWED_TYPES:      # 허용된 타엡에 없으면 무시
        return
    if not node_id:
        return

    if node_id not in nodes_by_id:  
        node = {
            "id": node_id,
            "type": node_type,
            "description": description or "",
        }
        if properties is not None:
            node["properties"] = properties
        nodes_by_id[node_id] = node
    else:   # 기존 노드에 description/properties가 없으면 보강
        if description and not nodes_by_id[node_id].get("description"):
            nodes_by_id[node_id]["description"] = description
        if properties is not None and "properties" not in nodes_by_id[node_id]:
            nodes_by_id[node_id]["properties"] = properties

def add_edge(edges, edge_key_set, source, target, rel_type, description=""):    # 엣지 추가
    if not source or not target:
        return
    if rel_type not in ALLOWED_REL_TYPES:
        return

    desc = (description or "").strip()
    key = (source, target, rel_type, desc)
    if key in edge_key_set:
        return
    edge_key_set.add(key)

    edge_id = f"{source}__{rel_type}__{target}"

    edges.append({  
        "id": edge_id,
        "source": source,
        "target": target,
        "relationship": rel_type,
        "type": rel_type,
        "description": desc,
        "label": desc,
        "title": desc,
        "tooltip": desc,
    })

def clean_text(s: str) -> str: 
    return (s or "").replace("\r\n", "\n").strip()

def parse_first_line_field(block: str, field_name: str) -> str | None:  # 예: "제목: xxx" 같은 1줄 필드 파싱
    m = re.search(rf"{re.escape(field_name)}\s*:\s*(.+)", block)
    return clean_text(m.group(1)) if m else None

def parse_body_fallback(block: str) -> str: # “본문:” 또는 “내용:” 뒤의 텍스트를 우선 사용
    """
    본문을 확실히 알 수 없으니,
    1) '본문:' 또는 '내용:' 같은 키워드가 있으면 그 뒤를 우선 사용
    2) 없으면 메일 블록 전체를 Q&A용 raw 형태로 body에 저장 (최소한 항상 본문이 존재하도록 보장)
    """
    # 1) 본문/내용 키워드 기반 (있는 경우)
    m = re.search(r"(본문|내용)\s*:\s*([\s\S]+)", block)
    if m:
        return clean_text(m.group(2))

    # 2) 없으면 블록 전체를 본문으로 (최소 보장)
    return clean_text(block)

def parse_attachments(block: str) -> list[str]:     # “첨부파일:” 또는 “첨부파일 정보:” 패턴을 기반으로 첨부파일 이름들을 추출
    """
    첨부파일 파싱:
    - "첨부파일:" 또는 "첨부파일 정보:" 라인이 있으면 우선 그 줄에서 파싱
    - 여러 줄로 들어오는 경우도 있을 수 있어서, '첨부파일' 섹션을 넉넉하게 수집
    - 결과는 파일명 리스트 (중복 제거)
    """
    attachments: list[str] = []

    # 1) "첨부파일:" 한 줄 형태
    for m in re.finditer(r"첨부파일(?:\s*정보)?\s*:\s*(.+)", block):
        line = clean_text(m.group(1))
        if not line:
            continue
        if "없음" in line or "없습니다" in line or "없다" in line:
            continue

        # 쉼표로 여러 개 올 수도 있어서 분리
        parts = [p.strip() for p in re.split(r"[,\uFF0C]", line) if p.strip()]
        for p in parts:
            # "파일명 (xxx)" 같은 경우 파일명만 추출
            name = p.split("(")[0].strip()
            if name:
                attachments.append(name)

    # 2) 여러 줄로 첨부파일이 들어오는 포맷 대응(보수적으로)
    # 예: "첨부파일 정보:" 다음 줄에 "- a.pdf" 같은 형태
    sec = re.search(r"첨부파일(?:\s*정보)?\s*:\s*\n([\s\S]+)", block)
    if sec:
        chunk = sec.group(1)
        for line in chunk.splitlines():
            t = line.strip().lstrip("-").strip()
            if not t:
                continue
            # 다른 필드가 시작되면 중단 (보수적)
            if re.match(r"^(ID|제목|보낸 사람|받는 사람|참조|날짜)\s*:", t):
                break
            if "없음" in t or "없습니다" in t or "없다" in t:
                continue
            name = t.split("(")[0].strip()
            if name:
                attachments.append(name)

    # 중복 제거(순서 유지)
    seen = set()
    out = []
    for a in attachments:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


# 메인 처리 루프
print("[INFO] cwd:", os.getcwd())
print("[INFO] reading:", MAIL_TXT_PATH)

if not os.path.exists(MAIL_TXT_PATH):
    raise FileNotFoundError(f"Mail file not found: {MAIL_TXT_PATH}")

with open(MAIL_TXT_PATH, "r", encoding="utf-8") as f:   
    all_text = f.read()

blocks = [b.strip() for b in all_text.split(MAIL_BLOCK_SEP) if b.strip() and "ID:" in b]   # 텍스트 전체를 MAIL_BLOCK_SEP로 split

nodes_by_id = {}    # 노드: id → node dict
edges = []          # 엣지: list
edge_key_set = set()    # 중복 방지용

for block in blocks:    # 각 메일 블록 처리
    mail_id = parse_first_line_field(block, "ID")   # 메일 아이디
    subject = parse_first_line_field(block, "제목") or "(제목 없음)"    # 제목
    sender = parse_first_line_field(block, "보낸 사람")     # 수신인
    receiver = parse_first_line_field(block, "받는 사람")   # 송신인

    if not mail_id:     # ID가 없으면 EMAIL 노드 만들기 애매하니 스킵
        continue

    # E-mail 노드 생성: 숨김 + Q&A용 본문/원문 저장
    email_node_id = f"EMAIL::{mail_id}"
    body_text = parse_body_fallback(block)

    safe_add_node(
        nodes_by_id,
        email_node_id,
        "EMAIL",
        description=f'Email "{subject}" (hidden, for QA).',
        properties={
            "mail_id": mail_id,
            "subject": subject,
            "from": sender or "",
            "to": receiver or "",
            "body": body_text,     # Q&A용 본문 (fallback 포함)
            "raw": clean_text(block),  # 원문 전체도 보관
        }
    )

    # SUBJECT 노드 (메일 제목 노드)
    safe_add_node(
        nodes_by_id,
        subject,
        "SUBJECT",
        description=f'Email subject "{subject}".'
    )

    # EMAIL ↔ SUBJECT 연결 (Q&A 컨텍스트 보강)
    add_edge(edges, edge_key_set, email_node_id, subject, "RELATES_TO",
             f'Email relates to subject "{subject}".')

    # PERSON 노드: 지금처럼 "이름 <메일주소>" 문자열 그대로
    if sender:
        safe_add_node(nodes_by_id, sender, "PERSON", f"{sender} appears in emails.")
        # 사람 ↔ EMAIL, 사람 ↔ SUBJECT 둘 다 연결(원하면 나중에 줄일 수 있음)
        add_edge(edges, edge_key_set, sender, email_node_id, "RELATES_TO",
                 f"{sender} is related to this email.")
        add_edge(edges, edge_key_set, sender, subject, "RELATES_TO",
                 f'{sender} is associated with "{subject}".')

    if receiver:
        safe_add_node(nodes_by_id, receiver, "PERSON", f"{receiver} appears in emails.")
        add_edge(edges, edge_key_set, receiver, email_node_id, "RELATES_TO",
                 f"{receiver} is related to this email.")
        add_edge(edges, edge_key_set, receiver, subject, "RELATES_TO",
                 f'{receiver} is associated with "{subject}".')

    # SENDS_TO (사람 → 사람 관계)
    if sender and receiver:
        add_edge(edges, edge_key_set, sender, receiver, "SENDS_TO",
                 f'{sender} sent an email to {receiver} about "{subject}".')

    # FILE 노드: 첨부파일 제목 노드 생성 + EMAIL과 연결
    attachment_names = parse_attachments(block)
    for fname in attachment_names:
        safe_add_node(nodes_by_id, fname, "FILE", f'Attachment file "{fname}".')
        add_edge(edges, edge_key_set, email_node_id, fname, "RELATES_TO",
                 f'Email includes attachment "{fname}".')
        # SUBJECT ↔ FILE 연결 (시각화용)
        add_edge(edges, edge_key_set,
                subject, fname, "RELATES_TO",
                f'Attachment "{fname}" belongs to subject "{subject}".')
    
# JSON 저장
all_nodes = list(nodes_by_id.values())
visible_nodes = [n for n in all_nodes if n.get("type") not in HIDDEN_STRUCT_TYPES]
visible_ids = {n["id"] for n in visible_nodes}

visible_edges = [
    e for e in edges
    if e["source"] in visible_ids and e["target"] in visible_ids
]

# 결과 파일 구조
graph_data = {
    "nodes": all_nodes,        # 전체 노드(E-mail 포함) 저장 → Q&A용
    "edges": edges,            # 전체 엣지 저장
    "visible_nodes": visible_nodes,  # 시각화용 (E-mail 제외 등)
    "visible_edges": visible_edges,  # 시각화용 따로 제공(선택)
}

ensure_dir_for_file(JSON_OUT_PATH)

with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:   # 쓰기 모드로 파일 오픈
    json.dump(graph_data, f, ensure_ascii=False, indent=2)  # graph_data 객체를, 유니코드 이스케이프 없이, 들여쓰기 2칸 포맷팅하여 저장

print(f"[OK] nodes={len(visible_nodes)}, edges={len(visible_edges)}")   # 저장 완료 메시지 (터미널에 출력)