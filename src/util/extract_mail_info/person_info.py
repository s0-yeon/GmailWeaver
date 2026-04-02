import os
import re
import json

# 이름+메일주소 형식에서 이름과 메일주소 분리하여 반환
def _parse_contact(raw: str) -> tuple[str, str]:
        m = re.search(r"^(.*?)\s*<([^>]+)>", raw.strip())
        if m:
            name  = m.group(1).strip().strip('"')
            email = m.group(2).strip().lower()
        else:
            name  = ""
            email = raw.strip().lower()
        return name, email

# 메일 블록에서 특정 필드 값 추출
def _extract_field(block: str, label: str) -> str:
        m = re.search(rf"^{label}:\s*(.+)$", block, re.MULTILINE)
        return m.group(1).strip() if m else ""

# 메일 발신 수신 횟수 계정별로 저장
def save_mail_contact_stats(blocks: list[str],paths, mode: str = "rewrite"):
    
    # 새로운 메일만 추가된 거라 이미 횟수 저장한 json 파일이 존재할 때 
    if mode == "append" and os.path.exists(paths.MAIL_STATICS_PATH):
        with open(paths.MAIL_STATICS_PATH, "r", encoding="utf-8") as f:
            stats = json.load(f)
    else: # 전체 갱신 모드일 때 빈 딕셔너리로 초기화해서 새로 횟수 셈
        stats = {}
    # 송수신 횟수 누적
    def add(name: str, email: str, direction: str):
        if not email or email in ("-", ""):
            return
        # 이메일 처음 등장하면 name, sent, received 초기화
        stats.setdefault(email, {"name": name, "sent": 0, "received": 0})
        # 이름이 있을 때 덮어씀
        if name:
            stats[email]["name"] = name
        stats[email][direction] += 1
    # 블록 순회하며 횟수 집계
    for block in blocks:
        direction = _extract_field(block, "구분") # 발신 또는 수신
        from_raw  = _extract_field(block, "발신인") # 발신인 원문
        to_raw    = _extract_field(block, "수신인") # 수신인 원문 
        subject   = _extract_field(block, "제목")

        # 본문 추출
        body = _extract_body(block)

        if direction == "발신":
            # 수신인 여러명이면 ,로 구분
            for addr in to_raw.split(","):
                name, email = _parse_contact(addr)
                add(name, email, "sent")
        elif direction == "수신":
            name, email = _parse_contact(from_raw)
            add(name, email, "received")

    # json 파일에 저장
    os.makedirs(os.path.dirname(paths.MAIL_STATICS_PATH), exist_ok=True)    
    with open(paths.MAIL_STATICS_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2) # indent=2 : 사람이 읽기 쉽게 들여쓰기 적용

    print(f"[STATS] ({mode}) 계정 {len(stats)}개 집계 완료 → {paths.MAIL_STATICS_PATH}")