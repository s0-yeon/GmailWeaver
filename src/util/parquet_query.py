import os
import re
import json
import pandas as pd
from dotenv import load_dotenv
import openai
from collections import Counter

load_dotenv("src/parquet/.env")
client = openai.OpenAI(api_key=os.environ.get("GRAPHRAG_API_KEY"))
PARQUET_DIR = "src/parquet/output"

def _load_entities():
    try:
        return pd.read_parquet(f"{PARQUET_DIR}/entities.parquet")
    except Exception as e:
        print(f"[load_entities error] {e}")
        return pd.DataFrame()

def _load_textunits():
    try:
        return pd.read_parquet(f"{PARQUET_DIR}/text_units.parquet")
    except Exception as e:
        print(f"[load_textunits error] {e}")
        return pd.DataFrame()

def _load_relationships():
    try:
        return pd.read_parquet(f"{PARQUET_DIR}/relationships.parquet")
    except Exception as e:
        print(f"[load_relationships error] {e}")
        return pd.DataFrame()

def _load_communities():
    try:
        return pd.read_parquet(f"{PARQUET_DIR}/community_reports.parquet")
    except Exception as e:
        print(f"[load_communities error] {e}")
        return pd.DataFrame()

def _sort_textunits(df: pd.DataFrame) -> pd.DataFrame:
    for col in ('human_readable_id', 'id', 'chunk_id'):
        if col in df.columns:
            return df.sort_values(col)
    return df

def _parse_all_emails() -> list[dict]:
    """전체 텍스트에서 모든 이메일 블록을 파싱해서 리스트로 반환"""
    textunits = _load_textunits()
    emails = []
    seen_ids = set()
    for _, row in textunits.iterrows():
        text = str(row['text'])
        blocks = re.split(r'={10,}', text)
        for block in blocks:
            mail_match = re.search(r'\[메일 (\d+)\]', block)
            if not mail_match:
                continue
            mail_num = int(mail_match.group(1))
            id_match      = re.search(r'ID:\s*(\S+)', block)
            subject_match = re.search(r'제목:\s*(.+)', block)
            sender_match  = re.search(r'보낸 사람:\s*(?:"([^"]+)"\s*)?<?([^>\n]+)>?', block)
            date_match    = re.search(r'날짜:\s*(.+)', block)
            body_match    = re.search(r'\[본문\]\s*(.*?)(?=\[|$)', block, re.DOTALL)

            mail_id = id_match.group(1) if id_match else f"num_{mail_num}"
            if mail_id in seen_ids:
                continue
            seen_ids.add(mail_id)

            sender_name  = sender_match.group(1).strip() if sender_match and sender_match.group(1) else ""
            sender_email = sender_match.group(2).strip() if sender_match and sender_match.group(2) else ""
            sender = sender_name or sender_email

            emails.append({
                "num":     mail_num,
                "id":      mail_id,
                "subject": subject_match.group(1).strip() if subject_match else "",
                "sender":  sender,
                "sender_email": sender_email,
                "date":    date_match.group(1).strip() if date_match else "",
                "body":    body_match.group(1).strip()[:300] if body_match else "",
                "raw":     block.strip()[:1000],
            })
    return sorted(emails, key=lambda x: x['num'])


# ── 1. Query Rewriting ─────────────────────────────────────────
REWRITE_PROMPT = """
당신은 이메일 검색 시스템의 쿼리 분석 전문가입니다.
사용자의 자연어 질문을 분석해서 최적의 검색 쿼리로 변환하세요.

반드시 JSON으로만 응답:
{
  "intent": "ENTITY",
  "primary_keyword": "소연코드테스트",
  "expanded_keywords": ["소연", "코드테스트", "소연 테스트"],
  "date_hint": "",
  "is_question_about_existence": true
}

intent 규칙:
- RECENCY    : 가장 최근/최신/오늘/마지막 이메일
- TEMPORAL   : 특정 날짜/기간 (3월, 어제, 오늘 등)
- ENTITY     : 특정 사람/조직/서비스/키워드
- STATISTICS : 개수/통계/몇 개/가장 많이/순위 관련 질문
- SUMMARY    : 전체 요약/목록/정리
- SEMANTIC   : 그 외 일반 검색

primary_keyword: 핵심 검색어 1개 (조사/어미 완전 제거)
expanded_keywords: 검색 정확도를 높이기 위한 변형/분리 키워드들
date_hint: 날짜 언급 있으면 추출, 없으면 ""
is_question_about_existence: "있어?", "있나요?" 같은 존재 확인 질문이면 true
"""

def rewrite_query(query: str) -> dict:
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            temperature=0,
            messages=[
                {"role": "system", "content": REWRITE_PROMPT},
                {"role": "user",   "content": query}
            ]
        )
        return json.loads(resp.choices[0].message.content)
    except Exception as e:
        print(f"[rewrite error] {e}")
        return {
            "intent": "SEMANTIC",
            "primary_keyword": query,
            "expanded_keywords": [],
            "date_hint": "",
            "is_question_about_existence": False
        }


# ── 2. Multi-Query 검색 ────────────────────────────────────────
def _search_entities(keywords: list) -> pd.DataFrame:
    entities = _load_entities()
    if entities.empty:
        return pd.DataFrame()
    results = []
    for kw in keywords:
        if not kw.strip():
            continue
        mask = (
            entities['title'].str.contains(kw, case=False, na=False) |
            entities['description'].str.contains(kw, case=False, na=False)
        )
        results.append(entities[mask])
    if not results:
        return pd.DataFrame()
    return pd.concat(results).drop_duplicates(subset=['title']).head(5)

def _search_textunits(keywords: list) -> pd.DataFrame:
    textunits = _load_textunits()
    if textunits.empty:
        return pd.DataFrame()
    results = []
    for kw in keywords:
        if not kw.strip():
            continue
        mask = textunits['text'].str.contains(kw, case=False, na=False)
        results.append(textunits[mask])
    if not results:
        return pd.DataFrame()
    return pd.concat(results).drop_duplicates(subset=['id']).head(3)


# ── 3. 의도별 검색 ─────────────────────────────────────────────
def _retrieve_recency() -> str:
    textunits = _load_textunits()
    if textunits.empty:
        return "정보 없음"
    for _, row in textunits.iterrows():
        text = str(row['text'])
        match = re.search(
            r'(\[메일 1\]\s*ID:.*?)(?=\[메일 \d+\]|={10,}|$)',
            text, re.DOTALL
        )
        if match:
            print(f"[Parquet] 메일 1 발견")
            return match.group(1).strip()[:1500]
    best_num = 99999
    best_block = ""
    for _, row in textunits.iterrows():
        text = str(row['text'])
        blocks = re.findall(
            r'(\[메일 (\d+)\]\s*ID:.*?)(?=\[메일 \d+\]|={10,}|$)',
            text, re.DOTALL
        )
        for block_text, num_str in blocks:
            num = int(num_str)
            if num < best_num:
                best_num = num
                best_block = block_text.strip()[:1500]
    print(f"[Parquet] fallback 메일 번호: {best_num}")
    return best_block if best_block else "정보 없음"

def _retrieve_temporal(hint: str) -> str:
    textunits = _load_textunits()
    if textunits.empty:
        return f"'{hint}' 관련 이메일 없음"
    mask = textunits['text'].str.contains(hint, na=False)
    rows = _sort_textunits(textunits[mask])
    if rows.empty:
        return f"'{hint}' 관련 이메일 없음"
    results = []
    for _, r in rows.iterrows():
        blocks = re.findall(
            r'\[메일 \d+\](.*?)(?=\[메일 \d+\]|={20,}|$)',
            r['text'], re.DOTALL
        )
        for b in blocks:
            if hint in b:
                results.append(b.strip()[:800])
    return "\n---\n".join(results) if results else str(rows.iloc[0]['text'])[:1500]

def _retrieve_statistics() -> str:
    """이메일 개수/발신자 통계 전용"""
    emails = _parse_all_emails()
    if not emails:
        return "통계 정보 없음"

    total = len(emails)
    sender_counter = Counter(e['sender'] for e in emails if e['sender'])
    top_senders = sender_counter.most_common(10)

    lines = [f"=== 총 이메일 수: {total}개 ===\n"]
    lines.append("=== 발신자별 이메일 수 (많은 순) ===")
    for sender, count in top_senders:
        lines.append(f"- {sender}: {count}개")

    lines.append("\n=== 최근 이메일 5개 ===")
    for e in emails[:5]:
        lines.append(f"[메일 {e['num']}] {e['date']} | {e['sender']} | {e['subject']}")

    return "\n".join(lines)

def _retrieve_by_keywords(keywords: list) -> str:
    lines = []
    matched_entities = _search_entities(keywords)
    if not matched_entities.empty:
        lines.append("=== 관련 엔티티 ===")
        relationships = _load_relationships()
        for _, e in matched_entities.iterrows():
            lines.append(f"[{e['title']}] {str(e.get('description',''))[:300]}")
            if not relationships.empty:
                rel_mask = (
                    relationships['source'].str.contains(str(e['title']), case=False, na=False) |
                    relationships['target'].str.contains(str(e['title']), case=False, na=False)
                )
                rels = relationships[rel_mask].head(5)
                for _, r in rels.iterrows():
                    lines.append(f"  → {str(r.get('description',''))[:150]}")

    # 키워드로 직접 이메일 검색
    emails = _parse_all_emails()
    matched_emails = []
    for e in emails:
        text = f"{e['subject']} {e['sender']} {e['body']}".lower()
        if any(kw.lower() in text for kw in keywords if kw):
            matched_emails.append(e)

    if matched_emails:
        lines.append("\n=== 관련 이메일 내용 ===")
        for e in matched_emails[:5]:
            lines.append(
                f"[메일 {e['num']}]\n"
                f"날짜: {e['date']}\n"
                f"보낸 사람: {e['sender']}\n"
                f"제목: {e['subject']}\n"
                f"본문: {e['body']}\n"
            )
    elif not lines:
        matched_texts = _search_textunits(keywords)
        if not matched_texts.empty:
            lines.append("\n=== 관련 이메일 내용 ===")
            for _, t in matched_texts.iterrows():
                lines.append(str(t['text'])[:500])

    return "\n".join(lines) if lines else "관련 정보 없음"

def _retrieve_summary() -> str:
    emails    = _parse_all_emails()
    communities = _load_communities()
    total = len(emails)
    sender_counter = Counter(e['sender'] for e in emails if e['sender'])
    top_senders = sender_counter.most_common(5)

    lines = [f"=== 총 이메일 수: {total}개 ==="]
    lines.append("\n=== 발신자별 이메일 수 ===")
    for sender, count in top_senders:
        lines.append(f"- {sender}: {count}개")

    lines.append("\n=== 커뮤니티 요약 ===")
    if not communities.empty:
        for _, c in communities.head(5).iterrows():
            title   = c.get('title', c.get('id', ''))
            summary = str(c.get('summary', ''))[:400]
            lines.append(f"[{title}]\n{summary}")

    lines.append("\n=== 최근 이메일 목록 ===")
    for e in emails[:10]:
        lines.append(f"[메일 {e['num']}] {e['date']} | {e['sender']} | {e['subject']}")

    return "\n".join(lines)


# ── 4. 메인 진입점 ─────────────────────────────────────────────
def build_context(query: str) -> str:
    rewritten = rewrite_query(query)
    intent    = rewritten.get("intent", "SEMANTIC")
    primary   = rewritten.get("primary_keyword", "").strip()
    expanded  = rewritten.get("expanded_keywords", [])
    date_hint = rewritten.get("date_hint", "").strip()

    all_keywords = list(dict.fromkeys([primary] + expanded))
    all_keywords = [k for k in all_keywords if k.strip()]

    print(f"[Parquet] intent={intent}, keywords={all_keywords}")

    if intent == "RECENCY":
        return _retrieve_recency()
    elif intent == "TEMPORAL":
        return _retrieve_temporal(date_hint or primary or query)
    elif intent == "STATISTICS":
        return _retrieve_statistics()
    elif intent == "SUMMARY":
        return _retrieve_summary()
    else:
        return _retrieve_by_keywords(all_keywords or [query])

def answer_query(query: str) -> str:
    context = build_context(query)
    print(f"[Parquet context 확인]\n{context[:500]}\n")

    if not context or context.strip() in ("정보 없음", "관련 정보 없음", "통계 정보 없음"):
        return "해당 내용을 찾을 수 없습니다."

    system_prompt = (
        "당신은 사용자의 이메일을 분석하는 AI 어시스턴트입니다.\n"
        "아래는 사용자 이메일에서 추출한 실제 데이터입니다.\n"
        "날짜·발신자·수신자·본문을 최대한 활용해 정확하고 구체적으로 한국어로 답변하세요.\n"
        "데이터에 없는 내용은 절대 추측하거나 예상하지 마세요.\n"
        "데이터가 없으면 '해당 내용을 찾을 수 없습니다'라고만 답하세요.\n\n"
        "=== 검색된 데이터 ===\n" + context
    )
    resp = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": query}
        ]
    )
    return resp.choices[0].message.content
