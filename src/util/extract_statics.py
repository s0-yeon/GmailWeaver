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

    type_col = 'type' if 'type' in entities_df.columns else 'entity_type'
    emails   = entities_df[entities_df[type_col].str.upper() == 'EMAIL']

    # relationships.parquet 기준: 실제 SENT_BY/SENT_TO가 있는 연락처만
    sent_by_count = rel_df[rel_df['description'] == 'SENT_BY'].groupby('target').size()
    sent_to_count = rel_df[rel_df['description'] == 'SENT_TO'].groupby('target').size()

    all_contacts = set(sent_by_count.index) | set(sent_to_count.index)
    all_contacts.discard(paths.GMAIL_ID.upper())   # 본인 제외

    # 이름 맵: entities.parquet Person 엔티티에서 파싱 (대문자 키)
    name_map = {}
    for _, row in entities_df[entities_df[type_col].str.upper() == 'PERSON'].iterrows():
        desc = str(row.get('description', ''))
        m = re.search(r'Name:\s*(.+)', desc)
        name = m.group(1).strip() if m else ''
        name_map[str(row['title']).upper()] = '' if name.lower() == 'none' else name

    # Tone: casual인 메일의 연락처별 친밀 카운트
    casual_ids = {
        str(row['title']) for _, row in emails.iterrows()
        if 'Tone: casual' in str(row.get('description', ''))
    }
    friendly_count = rel_df[rel_df['source'].isin(casual_ids)].groupby('target').size()

    for contact in all_contacts:
        email_lower = contact.lower()
        if mode == "append" and email_lower in stats:
            prev = stats[email_lower]
            stats[email_lower] = {
                "name":          name_map.get(contact.upper()) or prev.get("name", ""),
                "sent":          prev.get("sent", 0)          + int(sent_by_count.get(contact, 0)),
                "received":      prev.get("received", 0)      + int(sent_to_count.get(contact, 0)),
                "friendly_mail": prev.get("friendly_mail", 0) + int(friendly_count.get(contact, 0)),
            }
        else:
            stats[email_lower] = {
                "name":          name_map.get(contact.upper(), ""),
                "sent":          int(sent_by_count.get(contact, 0)),
                "received":      int(sent_to_count.get(contact, 0)),
                "friendly_mail": int(friendly_count.get(contact, 0)),
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


def generate_person_descriptions(paths) -> dict:
    """
    parquet에서 각 person의 이름·소속·주제·메일 수를 수집하고
    LLM으로 줄글 프로필을 생성해 dict로 반환한다 (DB 저장은 호출자가 담당).

    반환: { person_email: "이름: ...\n관계: ...\n자주 주고 받은 내용: ..." }
    """
    import pandas as pd

    if not os.path.exists(paths.ENTITIES_PATH) or not os.path.exists(paths.RELATIONSHIPS_PATH):
        print("[PROFILES] parquet 없음 → 프로필 생성 건너뜀")
        return {}

    entities_df = pd.read_parquet(paths.ENTITIES_PATH)
    rel_df      = pd.read_parquet(paths.RELATIONSHIPS_PATH)

    type_col = 'type' if 'type' in entities_df.columns else 'entity_type'

    def titles_of(etype: str) -> set:
        mask = entities_df[type_col].str.lower() == etype.lower()
        return set(entities_df.loc[mask, 'title'].astype(str))

    person_set = titles_of('person')
    topic_set  = titles_of('topic')
    org_set    = titles_of('organization')
    email_set  = titles_of('email')

    person_name_map   = {}
    topic_summary_map = {}
    org_name_map      = {}

    for _, row in entities_df.iterrows():
        etype = str(row.get(type_col, '')).lower()
        title = str(row.get('title', ''))
        desc  = str(row.get('description', ''))
        if etype == 'person':
            m = re.search(r'Name:\s*([^|]+)', desc)
            v = m.group(1).strip() if m else ''
            person_name_map[title] = '' if v.lower() == 'none' else v
        elif etype == 'topic':
            m = re.search(r'Summary:\s*(.+)', desc)
            topic_summary_map[title] = m.group(1).strip() if m else ''
        elif etype == 'organization':
            m = re.search(r'OrgName:\s*([^|]+)', desc)
            org_name_map[title] = m.group(1).strip() if m else title

    # mail_contact_stats.json 기준으로 대상 연락처 한정
    import json as _json
    contact_emails: set = set()
    if os.path.exists(paths.MAIL_CONTACTS_PATH):
        with open(paths.MAIL_CONTACTS_PATH, "r", encoding="utf-8") as _f:
            contact_emails = set(_json.load(_f).keys())  # lowercase

    email_to_topics:  dict[str, list] = {}
    person_to_emails: dict[str, set]  = {p: set() for p in person_set}
    person_to_orgs:   dict[str, set]  = {p: set() for p in person_set}
    person_counts:    dict[str, dict] = {
        p: {'sent': 0, 'received': 0, 'cc': 0} for p in person_set
    }

    for _, row in rel_df.iterrows():
        src   = str(row.get('source', ''))
        tgt   = str(row.get('target', ''))
        # description 컬럼이 관계 타입 (SENT_BY, SENT_TO, CC_TO, ...)
        rtype = str(row.get('description', '')).upper()

        if src in email_set and tgt in topic_set:
            email_to_topics.setdefault(src, []).append(tgt)

        if src in email_set and tgt in person_set:
            person_to_emails[tgt].add(src)
            if   rtype == 'SENT_BY':  person_counts[tgt]['sent']     += 1
            elif rtype == 'SENT_TO':  person_counts[tgt]['received'] += 1
            elif rtype == 'CC_TO':    person_counts[tgt]['cc']       += 1
            else:                     person_counts[tgt]['received'] += 1

        if src in person_set and tgt in org_set:
            person_to_orgs[src].add(org_name_map.get(tgt, tgt))

    descriptions: dict[str, str] = {}
    my_email = paths.GMAIL_ID.lower()

    for person_email in person_set:
        if person_email.lower() == my_email:
            continue
        if person_email.lower() not in contact_emails:
            continue

        counts      = person_counts[person_email]
        total_mails = counts['sent'] + counts['received'] + counts['cc']

        name = person_name_map.get(person_email, '')
        orgs = list(person_to_orgs[person_email])

        topic_counter: dict[str, int] = {}
        for eid in person_to_emails[person_email]:
            for t in email_to_topics.get(eid, []):
                topic_counter[t] = topic_counter.get(t, 0) + 1

        top_topics  = sorted(topic_counter, key=topic_counter.get, reverse=True)[:5]
        topics_text = '\n'.join(
            f"- {t}: {topic_summary_map.get(t, t)}" for t in top_topics
        ) or '(주제 정보 없음)'

        prompt = f"""다음은 이메일 분석 데이터입니다.

나의 이메일: {my_email}
상대방 이메일: {person_email}
이름: {name if name else '알 수 없음'}
소속 조직: {', '.join(orgs) if orgs else '없음'}
주고받은 메일 수: {total_mails}건 (보낸 {counts['sent']}건 / 받은 {counts['received']}건)
주요 대화 주제:
{topics_text}

아래 형식으로만 출력하세요. 다른 텍스트는 절대 포함하지 마세요.
관계: <이 사람과 나의 관계를 한 문장으로>
자주 주고 받은 내용: <주로 어떤 내용으로 메일을 주고받는지 한 문장으로>""".strip()

        try:
            result = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 이메일 데이터를 분석해 인물 관계를 한국어로 간결하게 요약하는 AI입니다."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            llm_output = result.choices[0].message.content.strip()
        except Exception as e:
            print(f"[PROFILES] LLM 호출 실패 ({person_email}): {e}")
            continue

        rel_m     = re.search(r'관계:\s*(.+)',            llm_output)
        content_m = re.search(r'자주 주고 받은 내용:\s*(.+)', llm_output)

        relationship = rel_m.group(1).strip()     if rel_m     else ''
        content      = content_m.group(1).strip() if content_m else ''

        descriptions[person_email] = (
            f"이름: {name if name else '알 수 없음'}\n"
            f"관계: {relationship}\n"
            f"자주 주고 받은 내용: {content}"
        )
        print(f"[PROFILES] 완료: {person_email}")

    print(f"[PROFILES] 총 {len(descriptions)}명 프로필 생성 완료")
    return descriptions


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