import os
import re
import json
import time
import threading
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

# LLM으로 친밀한 어조 판별
def _is_friendly_tone_with_llm(body: str) -> bool:

    if not body.strip():
        return False
    
    body = body[:1500]

    prompt = f"""
    다음 메일 본문이 '친밀한 어조'인지 판별하라.

    판별 기준:
    - '친밀한 어조'란, 개인적인 친분이나 가까운 관계가 느껴지는 말투를 뜻한다.
    - 사적인 안부, 다정한 표현, 편한 말투, 친한 사이에서 쓰는 표현이 중심이면 friendly다.
    - 단순히 예의 바르거나 친절한 것만으로는 friendly가 아니다.
    - 업무 메일, 학교 메일, 공지, 안내, 광고, 자동 발송, 고객 응대, 형식적인 감사 표현은 not_friendly다.
    - "감사합니다", "좋은 하루 되세요", "잘 부탁드립니다" 같은 일반적인 공손 표현만 있으면 not_friendly다.
    - 메일 전체 분위기가 공식적이거나 정보 전달 중심이면 not_friendly다.

    반드시 아래 둘 중 하나만 정확히 출력하라.
    friendly
    not_friendly

    메일 본문:
    {body}
    """.strip()

    result = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "당신은 메일 본문의 어조를 분류하는 AI입니다. 반드시 friendly 또는 not_friendly 둘 중 하나만 출력하세요."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0
    )

    answer = result.choices[0].message.content.strip().lower()
    return answer == "friendly"

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
def _save_mail_contact_stats(paths, mode: str = "rewrite"):
    import pandas as pd

    if not os.path.exists(paths.ENTITIES_PATH) or not os.path.exists(paths.RELATIONSHIPS_PATH):
        print(f"[STATS] entities/relationships parquet 없음 → contacts 생성 건너뜀")
        return

    if mode == "append" and os.path.exists(paths.MAIL_CONTACTS_PATH):
        with open(paths.MAIL_CONTACTS_PATH, "r", encoding="utf-8") as f:
            stats = json.load(f)
    else:
        stats = {}

    entities_df = pd.read_parquet(paths.ENTITIES_PATH)
    rel_df      = pd.read_parquet(paths.RELATIONSHIPS_PATH)

    persons = entities_df[entities_df['type'] == 'PERSON']
    emails  = entities_df[entities_df['type'] == 'EMAIL']

    # SENT_BY = 발신인, SENT_TO = 수신인
    sent_by_count = rel_df[rel_df['description'].str.contains('SENT_BY', na=False)].groupby('target').size()
    sent_to_count = rel_df[rel_df['description'].str.contains('SENT_TO', na=False)].groupby('target').size()

    # Tone: casual인 메일 ID 수집
    casual_email_ids = set()
    for _, row in emails.iterrows():
        if 'Tone: casual' in str(row.get('description', '')):
            casual_email_ids.add(row['title'])

    # casual 메일에 연결된 person별 카운트
    casual_rel = rel_df[rel_df['source'].isin(casual_email_ids)]
    friendly_count = casual_rel.groupby('target').size()

    for _, row in persons.iterrows():
        email_addr = row['title'].lower()
        desc = str(row.get('description', ''))
        name_match = re.search(r'Name:\s*(.+)', desc)
        name = name_match.group(1).strip() if name_match else ''

        stats[email_addr] = {
            "name": name,
            "sent":          int(sent_by_count.get(row['title'], 0)),
            "received":      int(sent_to_count.get(row['title'], 0)),
            "friendly_mail": int(friendly_count.get(row['title'], 0)),
        }

    with open(paths.MAIL_CONTACTS_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"[STATS] ({mode}) 계정 {len(stats)}개 집계 완료 → {paths.MAIL_CONTACTS_PATH}")

def _save_mail_keyword_stats(paths, mode: str = "rewrite"):
    import pandas as pd, re
    # 기존 데이터 로드 (append 모드)
    if mode == "append" and os.path.exists(paths.MAIL_KEYWORDS_PATH):
        with open(paths.MAIL_KEYWORDS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            keyword_stats = data.get("keywords", {})
            keyword_person_date_map = data.get("keyword_person_date_map", {})
            processed_ids = set(data.get("processed_mail_ids", []))
    else:
        keyword_stats = {}
        keyword_person_date_map = {}
        processed_ids = set()

    text_units_df = pd.read_parquet(paths.RELATIONSHIPS_PATH.replace("relationships.parquet", "text_units.parquet"))

    for _, row in text_units_df.iterrows():
        text = str(row.get('text', ''))

        id_match = re.search(r'^ID:\s*(.+)$', text, re.MULTILINE)
        mail_id = id_match.group(1).strip() if id_match else None

        if mode == "append" and mail_id in processed_ids:
            continue

        date_match = re.search(r'^날짜:\s*(.+)$', text, re.MULTILINE)
        mail_date = date_match.group(1).strip()[:10] if date_match else None  # YYYY-MM-DD

        def parse_email(value):
            m = re.search(r'<(.+?)>', value)
            return m.group(1).strip() if m else value.strip()

        sender_match = re.search(r'^발신인:\s*(.+)$', text, re.MULTILINE)
        sender = parse_email(sender_match.group(1)) if sender_match else None

        receiver_match = re.search(r'^수신인:\s*(.+)$', text, re.MULTILINE)
        receiver = parse_email(receiver_match.group(1)) if receiver_match else None

        person = receiver if sender == paths.GMAIL_ID else sender

        body_match = re.search(r'\[메일 본문\]\s*\n(.*?)(?:\n=+|\Z)', text, re.DOTALL)
        body = body_match.group(1).strip() if body_match else ''

        if not body or not mail_date or not person:
            continue

        keywords = extract_keywords_with_llm(body)

        for kw in keywords:
            keyword_stats[kw] = keyword_stats.get(kw, 0) + 1
            if kw not in keyword_person_date_map:
                keyword_person_date_map[kw] = {}
            if person not in keyword_person_date_map[kw]:
                keyword_person_date_map[kw][person] = {}
            keyword_person_date_map[kw][person][mail_date] = \
                keyword_person_date_map[kw][person].get(mail_date, 0) + 1

        if mail_id:
            processed_ids.add(mail_id)

    result = {
        "keywords": keyword_stats,
        "keyword_person_date_map": keyword_person_date_map,
        "processed_mail_ids": list(processed_ids)
    }

    with open(paths.MAIL_KEYWORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[KEYWORD] ({mode}) 키워드 {len(keyword_stats)}개 저장 완료 → {paths.MAIL_KEYWORDS_PATH}")


def _extract_statics_pipeline(paths, mode: str = "rewrite"):
    os.makedirs(paths.MAIL_STATICS_PATH, exist_ok=True)
    _save_mail_keyword_stats(paths, mode)
    _save_mail_contact_stats(paths, mode)

def run_statics_pipeline(job_id, paths, mode: str = "rewrite"):
    print(f"[JOB][statics] START job_id={job_id}")
    append_job_log(job_id, "[START] statics pipeline")

    try:
        update_job(job_id, status="running", progress=0, message="통계 추출 시작")
        _extract_statics_pipeline(paths, mode)

        update_job(
            job_id,
            status="done",
            progress=100,
            message="통계 추출 완료",
            result={
                "mail_keywords_path": paths.MAIL_KEYWORDS_PATH,
                "mail_contacts_path": paths.MAIL_CONTACTS_PATH,
                "mode": mode,
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

def start_statics_pipeline_background(job_id, paths, mode: str = "rewrite"):
    print(f"[JOB][statics] BACKGROUND START job_id={job_id}")
    append_job_log(job_id, "[INFO] background thread starting")

    t = threading.Thread(
        target=run_statics_pipeline,
        args=(job_id, paths, mode),
        daemon=True,
    )
    t.start()

    print(f"[JOB][statics] BACKGROUND THREAD STARTED job_id={job_id} thread={t.name}")
    append_job_log(job_id, f"[INFO] background thread started name={t.name}")

    return t