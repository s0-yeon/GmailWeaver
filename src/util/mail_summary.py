import os
import re
import json
import datetime
import openai
from dotenv import load_dotenv

load_dotenv("src/parquet/.env")


def _extract_field(text, field_name):
    m = re.search(rf'^{re.escape(field_name)}:\s*(.+)$', text, re.MULTILINE)
    return m.group(1).strip() if m else None


def _extract_body(text):
    m = re.search(r'\[메일 본문\]\s*\n(.*?)(?=\n\[|$)', text, re.DOTALL)
    return m.group(1).strip() if m else ""


def _summarize_with_llm(text, period_label):
    client = openai.OpenAI(api_key=os.environ.get("GRAPHRAG_API_KEY"))
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "주어진 이메일 목록을 분석하여 해당 기간의 주요 메일 내용을 3~5문장으로 한국어로 요약하세요. "
                        "주요 주제, 주고받은 사람, 중요한 사안을 중심으로 요약해주세요."
                    )
                },
                {
                    "role": "user",
                    "content": f"[{period_label}] 메일 목록:\n\n{text}"
                }
            ],
            max_tokens=300
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[mail_summary] LLM 오류 ({period_label}): {e}")
        return ""


def generate_mail_summaries(paths):
    import pandas as pd

    text_units_path = paths.RELATIONSHIPS_PATH.replace("relationships.parquet", "text_units.parquet")
    if not os.path.exists(text_units_path):
        print(f"[mail_summary] text_units.parquet 없음: {text_units_path}")
        return

    df = pd.read_parquet(text_units_path)

    mails = []
    seen_ids = set()

    for _, row in df.iterrows():
        text = str(row.get('text', ''))

        mail_id = _extract_field(text, 'ID')
        if not mail_id or mail_id in seen_ids:
            continue
        seen_ids.add(mail_id)

        date_str = _extract_field(text, '날짜')
        if not date_str:
            continue

        try:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        mails.append({
            "date":    date,
            "year":    date.strftime("%Y"),
            "month":   date.strftime("%Y-%m"),
            "subject": _extract_field(text, '제목') or "",
            "sender":  _extract_field(text, '발신인') or "",
            "body":    _extract_body(text)[:500],
        })

    if not mails:
        print("[mail_summary] 요약할 메일 없음")
        return

    mails.sort(key=lambda x: x["date"])

    monthly_groups = {}
    yearly_groups  = {}
    for mail in mails:
        monthly_groups.setdefault(mail["month"], []).append(mail)
        yearly_groups.setdefault(mail["year"],  []).append(mail)

    def _build_text(group):
        return "\n\n".join(
            f"제목: {m['subject']}\n발신인: {m['sender']}\n내용: {m['body']}"
            for m in group
        )

    monthly_summaries = {}
    for month, group in monthly_groups.items():
        print(f"[mail_summary] 월별 요약 중: {month} ({len(group)}건)")
        monthly_summaries[month] = _summarize_with_llm(_build_text(group), month)

    yearly_summaries = {}
    for year, group in yearly_groups.items():
        print(f"[mail_summary] 연별 요약 중: {year} ({len(group)}건)")
        yearly_summaries[year] = _summarize_with_llm(_build_text(group), year)

    result = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "yearly":  yearly_summaries,
        "monthly": monthly_summaries,
    }

    os.makedirs(paths.MAIL_STATICS_PATH, exist_ok=True)
    with open(paths.MAIL_SUMMARIES_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[mail_summary] 저장 완료: {paths.MAIL_SUMMARIES_PATH}")
