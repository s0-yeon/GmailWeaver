import os
import re
import json
import time
import traceback
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
# Job 이용 공통함수 import
from util.jobs.job_store import *
# .env 로드
load_dotenv("src/parquet/.env")

client = OpenAI(api_key=os.getenv("GRAPHRAG_API_KEY"))

def start_timer():
    return {
        "started_at": datetime.now(),
        "start_perf": time.perf_counter()
    }

def end_timer(timer):
    ended_at = datetime.now()
    elapsed_sec = time.perf_counter() - timer["start_perf"]

    return {
        "started_at": timer["started_at"],
        "ended_at": ended_at,
        "elapsed_sec": round(elapsed_sec, 2)
    }

def format_elapsed_time(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60  # 소수 포함

    return f"{hours:02d}:{minutes:02d}:{secs:05.2f}"

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
def _extract_field(block: str, label: str, multiline: bool = False) -> str:
    if multiline:
        m = re.search(
            rf"\[{re.escape(label)}\]\s*\n(.*?)(?:\n=+|\Z)",
            block,
            re.DOTALL
        )
    else:
        m = re.search(
            rf"^{re.escape(label)}:\s*(.+)$",
            block,
            re.MULTILINE
        )
    return m.group(1).strip() if m else ""

def extract_keywords_with_llm(body: str) -> list[str]:
    body = body.strip()
    if not body:
        return []

    body = body[:2000]

    prompt = f"""
다음 메일 본문에서 핵심 키워드를 최대 3개만 추출하세요.

조건:
- 한국어 명사 위주
- 중복 금지
- 너무 일반적인 단어(예: 내용, 경우, 사람) 제외
- JSON 배열로만 출력
- 내용에서 핵심적인 단어만
- 예시: ["보안", "계정", "액세스"]

메일 본문:
{body}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "당신은 텍스트에서 핵심 키워드를 추출하는 AI입니다."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )

        result_text = response.choices[0].message.content.strip()

        if result_text.startswith("```"):
            result_text = re.sub(r"^```(?:json)?\s*", "", result_text)
            result_text = re.sub(r"\s*```$", "", result_text)

        keywords = json.loads(result_text)

        if not isinstance(keywords, list):
            print("[LLM DEBUG] list가 아님")
            return []

        cleaned = []
        for kw in keywords:
            if isinstance(kw, str):
                kw = kw.strip()
                if kw and kw not in cleaned:
                    cleaned.append(kw)

        return cleaned[:5]

    except Exception as e:
        print(f"[LLM ERROR] 키워드 추출 실패: {e}")
        return []


# 메일 발신 수신 횟수 계정별로 저장
def _save_mail_contact_stats(blocks: list[str],paths, mode: str = "rewrite"):
    # 새로운 메일만 추가된 거라 이미 횟수 저장한 json 파일이 존재할 때 
    if mode == "append" and os.path.exists(paths.MAIL_CONTACTS_PATH):
        with open(paths.MAIL_CONTACTS_PATH, "r", encoding="utf-8") as f:
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

        if direction == "발신":
            # 수신인 여러명이면 ,로 구분
            for addr in to_raw.split(","):
                name, email = _parse_contact(addr)
                add(name, email, "sent")
        elif direction == "수신":
            name, email = _parse_contact(from_raw)
            add(name, email, "received")

    # json 파일에 저장
    #os.makedirs(os.path.dirname(paths.MAIL_STATICS_PATH), exist_ok=True)    
    with open(paths.MAIL_CONTACTS_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2) # indent=2 : 사람이 읽기 쉽게 들여쓰기 적용

    print(f"[STATS] ({mode}) 계정 {len(stats)}개 집계 완료 → {paths.MAIL_CONTACTS_PATH}")

def _save_mail_keyword_stats(blocks: list[str], paths, mode: str = "rewrite"):
    # 기존 데이터 로드 (append 모드)
    if mode == "append" and os.path.exists(paths.MAIL_KEYWORDS_PATH):
        with open(paths.MAIL_KEYWORDS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            keyword_stats = data.get("keywords", {})
            processed_ids = set(data.get("processed_mail_ids", []))
    else:
        keyword_stats = {}
        processed_ids = set()

    # 키워드 누적 함수
    def add_keywords(keywords: list[str]):
        if not keywords:
            return
        for kw in keywords:
            keyword_stats[kw] = keyword_stats.get(kw, 0) + 1

    # 블록 순회
    for block in blocks:
        mail_id = _extract_field(block, "ID")

        # 이미 처리한 메일이면 skip (append 모드 )
        if mode == "append" and mail_id in processed_ids:
            continue

        body = _extract_field(block, "메일 본문", multiline=True)

        if not body:
            continue

        # 🔥 LLM 키워드 추출
        keywords = extract_keywords_with_llm(body)

        # 키워드 집계
        add_keywords(keywords)

        # 처리된 메일 기록
        if mail_id:
            processed_ids.add(mail_id)

    # 저장 구조
    result = {
        "keywords": keyword_stats,
        "processed_mail_ids": list(processed_ids)
    }

    #os.makedirs(os.path.dirname(paths.MAIL_KEYWORDS_PATH), exist_ok=True)

    with open(paths.MAIL_KEYWORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[KEYWORD] ({mode}) 키워드 {len(keyword_stats)}개 저장 완료 → {paths.MAIL_KEYWORDS_PATH}")


def _extract_statics_pipeline(blocks: list[str], paths, mode: str = "rewrite"):
    print("[DEBUG] blocks 개수:", len(blocks))

    os.makedirs(paths.MAIL_STATICS_PATH, exist_ok=True)
    _save_mail_keyword_stats(blocks, paths, mode)
    _save_mail_contact_stats(blocks, paths, mode)

def run_statics_pipeline(job_id, blocks: list[str], paths, mode: str = "rewrite"):
    print(f"[JOB][statics] START job_id={job_id}")
    append_job_log(job_id, "[START] statics pipeline")

    print(f"[JOB][statics] START job_id={job_id}")
    append_job_log(job_id, "[START] statics pipeline")

    try:
        update_job(job_id, status="running", progress=0, message="통계 추출 시작")
        _extract_statics_pipeline(blocks, paths, mode)

        update_job(
            job_id,
            status="done",
            progress=100,
            message="통계 추출 완료",
            result={
                "mail_keywords_path": paths.MAIL_KEYWORDS_PATH,
                "mail_contacts_path": paths.MAIL_CONTACTS_PATH,
                "mode": mode,
                "blocks_count": len(blocks),
            },
            finished_at=time.time(),
        )
        append_job_log(job_id, "[FINISH] statics pipeline completed")

    except Exception as e:
        err_text = f"{type(e).__name__}: {e}"
        append_job_log(job_id, f"[ERROR] {err_text}")
        update_job(
            job_id,
            status="failed",
            progress=100,
            message="통계 추출 실패",
            error=err_text,
            finished_at=time.time(),
        )

def start_statics_pipeline_background(job_id, blocks: list[str], paths, mode: str = "rewrite"):
    print(f"[JOB][statics] BACKGROUND START job_id={job_id}")
    append_job_log(job_id, "[INFO] background thread starting")

    t = threading.Thread(
        target=run_statics_pipeline,
        args=(job_id, blocks, paths, mode),
        daemon=True,
    )
    t.start()

    print(f"[JOB][statics] BACKGROUND THREAD STARTED job_id={job_id} thread={t.name}")
    append_job_log(job_id, f"[INFO] background thread started name={t.name}")

    return t

