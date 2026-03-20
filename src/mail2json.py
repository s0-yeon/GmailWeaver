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
    "LABEL", # 라벨명
}

ALLOWED_REL_TYPES = {   # 허용된 엣지 종류
    "SENDS_TO",
    "RELATES_TO",
}

MAIL_BLOCK_SEP = "============================================================"     # 메일 블록 구분자

# 공통 헬퍼 함수
# 출력 경로 보장
def ensure_dir_for_file(file_path: str):    
    dir_path = os.path.dirname(file_path)   # 파일명 제거하고 경로만 추출
    if dir_path:    
        os.makedirs(dir_path, exist_ok=True)    

# 저장 및 노드 추가
def safe_add_node(
        nodes_by_id: Dict[str, dict], 
        node_id: str, 
        node_type: str, 
        description: str = "", 
        properties: Optional[Dict[str, Any]] = None 
    ):
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
    else:   
        # description 보강
        if description and not nodes_by_id[node_id].get("description"):
            nodes_by_id[node_id]["description"] = description

        # properties 병합 보강
        if properties is not None:
            existing = nodes_by_id[node_id].get("properties", {})
            merged = dict(existing)
            for k, v in properties.items():
                if v not in (None, "", [], {}):
                    merged[k] = v
                elif k not in merged:
                    merged[k] = v
            nodes_by_id[node_id]["properties"] = merged

# 엣지 추가
def add_edge(edges, edge_key_set, source, target, rel_type, description=""):    
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

# 텍스트 지우기
def clean_text(s: str) -> str: 
    return (s or "").replace("\r\n", "\n").strip()

# 첫 번째 라인 필드 파싱
def parse_first_line_field(block: str, field_name: str) -> str | None:  # 예: "제목: xxx" 같은 1줄 필드 파싱
    m = re.search(rf"{re.escape(field_name)}\s*:\s*(.+)", block)
    return clean_text(m.group(1)) if m else None

# [라벨 정보], [첨부파일 정보], [본문], [첨부 추출 내용] 같은 섹션 내용을 추출
def parse_section(block: str, section_name: str) -> str:
    pattern = rf"\[{re.escape(section_name)}\]\s*\n([\s\S]*?)(?=\n\[[^\n]+\]\s*\n|$)"
    m = re.search(pattern, block)
    return clean_text(m.group(1)) if m else ""

# 본문 내용 대체. “본문:” 또는 “내용:” 뒤의 텍스트를 우선 사용
def parse_body_fallback(block: str) -> str:
    section_body = parse_section(block, "본문")
    if section_body:
        return section_body

    m = re.search(r"(본문|내용)\s*:\s*([\s\S]+)", block)
    if m:
        return clean_text(m.group(2))

    return clean_text(block)

# 라벨 파싱 (라벨 정보, 프로젝트, 중요, INBOX)
def parse_labels(block: str) -> list[str]:

    label_text = parse_section(block, "라벨 정보")
    if not label_text or label_text in {"없음", "없습니다", "없다"}:
        return []

    labels = []
    for part in re.split(r"[,\uFF0C\n]+", label_text):
        t = clean_text(part)
        if t and t not in {"없음", "없습니다", "없다"}:
            labels.append(t)

    # 중복 제거(순서 유지)
    seen = set()
    out = []
    for label in labels:
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out


 # 첨부파일 메타정보 파싱
def parse_attachment_infos(block: str) -> list[dict]:
    section = parse_section(block, "첨부파일 정보")
    if not section:
        return []

    if "첨부파일: 없음" in section or section.strip() in {"없음", "없습니다", "없다"}:
        return []

    infos = []
    for line in section.splitlines():
        t = line.strip()
        if not t:
            continue
        if t.startswith("첨부파일"):
            continue

        # "1. 파일명 | mime | size | status"
        m = re.match(r"^\d+\.\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*$", t)
        if m:
            infos.append({
                "name": clean_text(m.group(1)),
                "mime": clean_text(m.group(2)),
                "size": clean_text(m.group(3)),
                "status": clean_text(m.group(4)),
            })
            continue

        # 혹시 상태 정보 없는 구버전
        m2 = re.match(r"^\d+\.\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*$", t)
        if m2:
            infos.append({
                "name": clean_text(m2.group(1)),
                "mime": clean_text(m2.group(2)),
                "size": clean_text(m2.group(3)),
                "status": "",
            })

    # name 기준 중복 제거
    seen = set()
    out = []
    for info in infos:
        name = info.get("name", "")
        if name and name not in seen:
            seen.add(name)
            out.append(info)

    return out

# 하위 호환용: 첨부파일명 리스트만 필요할 때 사용
def parse_attachments(block: str) -> list[str]:
    return [info["name"] for info in parse_attachment_infos(block) if info.get("name")]


# 첨부 추출 텍스트 파싱
def parse_attachment_extracted_texts(block: str) -> Dict[str, str]:
    section = parse_section(block, "첨부 추출 내용")
    if not section:
        return {}

    result: Dict[str, str] = {}

    pattern = r"\[File name\]\s*(.+?)\n([\s\S]*?)(?=\n\[File name\]\s*.+?\n|$)"
    for m in re.finditer(pattern, section):
        fname = clean_text(m.group(1))
        text = clean_text(m.group(2))
        if fname:
            result[fname] = text

    return result

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
    mail_id = parse_first_line_field(block, "ID")
    subject = parse_first_line_field(block, "제목") or "(제목 없음)"
    sender = parse_first_line_field(block, "보낸 사람")
    receiver = parse_first_line_field(block, "받는 사람")
    cc = parse_first_line_field(block, "참조(CC)")
    date = parse_first_line_field(block, "날짜")

    if not mail_id:     # ID가 없으면 EMAIL 노드 만들기 애매하니 스킵
        continue

    # E-mail 노드 생성: 숨김 + Q&A용 본문/원문 저장
    email_node_id = f"EMAIL::{mail_id}"
    body_text = parse_body_fallback(block)
    raw_text = clean_text(block)

    labels = parse_labels(block)
    attachment_infos = parse_attachment_infos(block)
    attachment_texts = parse_attachment_extracted_texts(block)

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
            "cc": cc or "",
            "date": date or "",
            "labels": labels,
            "body": body_text,
            "raw": raw_text,
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

    # SENDS_TO 노드: 사람 → 사람 관계
    if sender and receiver:
        add_edge(edges, edge_key_set, sender, receiver, "SENDS_TO",
                 f'{sender} sent an email to {receiver} about "{subject}".')

    # LABEL 노드: 라벨 노드 생성 + 메일 제목과 연결
    for label in labels:
        label_node_id = f"LABEL::{label}"
        safe_add_node(
            nodes_by_id,
            label_node_id,
            "LABEL",
            description=f'Label "{label}".',
            properties={"name": label}
        )
        add_edge(
            edges, edge_key_set,
            email_node_id, label_node_id, "RELATES_TO",
            f'Email has label "{label}".'
        )
        add_edge(
            edges, edge_key_set,
            subject, label_node_id, "RELATES_TO",
            f'Subject "{subject}" is associated with label "{label}".'
        )

    # FILE 노드: 첨부파일 제목 노드 생성 + EMAIL과 연결
    for att in attachment_infos:
        fname = att.get("name", "")
        if not fname:
            continue

        extracted_text = attachment_texts.get(fname, "")

        safe_add_node(
            nodes_by_id,
            fname,
            "FILE",
            description=f'Attachment file "{fname}".',
            properties={
                "name": fname,
                "mime": att.get("mime", ""),
                "size": att.get("size", ""),
                "status": att.get("status", ""),
                "text": extracted_text,
            }
        )

        add_edge(
            edges, edge_key_set,
            email_node_id, fname, "RELATES_TO",
            f'Email includes attachment "{fname}".'
        )
        add_edge(
            edges, edge_key_set,
            subject, fname, "RELATES_TO",
            f'Attachment "{fname}" belongs to subject "{subject}".'
        )
    
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