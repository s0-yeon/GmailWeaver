import os
import re
import time
import datetime
import calendar
import pandas as pd
import openai

def _extract_date_range(message: str):
    today = datetime.datetime.now()
    year = today.year

    # 패턴1: 3월 22일 ~ 3월 25일
    m = re.search(r'(\d+)월\s*(\d+)일\s*[~～\-]\s*(\d+)월\s*(\d+)일', message)
    if m:
        start = f"{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
        end = f"{year}-{int(m.group(3)):02d}-{int(m.group(4)):02d}"
        return start, end

    # 패턴2: 3월 22일 ~ 25일
    m = re.search(r'(\d+)월\s*(\d+)일\s*[~～\-]\s*(\d+)일', message)
    if m:
        month = int(m.group(1))
        start = f"{year}-{month:02d}-{int(m.group(2)):02d}"
        end = f"{year}-{month:02d}-{int(m.group(3)):02d}"
        return start, end

    # 패턴3: 2026-03-22 ~ 2026-03-25
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})\s*[~～\-]\s*(\d{4})-(\d{2})-(\d{2})', message)
    if m:
        start = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        end = f"{m.group(4)}-{m.group(5)}-{m.group(6)}"
        return start, end

    # 패턴4: 2026-03-22 ~ 03-25
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})\s*[~～\-]\s*(\d{2})-(\d{2})', message)
    if m:
        start = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        end = f"{m.group(1)}-{m.group(4)}-{m.group(5)}"
        return start, end

    # 패턴5: 이번 주
    if '이번 주' in message or '이번주' in message:
        start_of_week = today - datetime.timedelta(days=today.weekday())
        end_of_week = start_of_week + datetime.timedelta(days=6)
        return start_of_week.strftime('%Y-%m-%d'), end_of_week.strftime('%Y-%m-%d')

    # 패턴6: 지난 주 / 저번 주 / 저저번 주 (N주 전)
    m = re.search(r'(저저번|저번|지난)\s*주', message)
    if m:
        weeks_ago = 2 if m.group(1) == '저저번' else 1
        start_of_week = today - datetime.timedelta(days=today.weekday() + 7 * weeks_ago)
        end_of_week = start_of_week + datetime.timedelta(days=6)
        return start_of_week.strftime('%Y-%m-%d'), end_of_week.strftime('%Y-%m-%d')

    # 패턴7: 오늘
    if '오늘' in message:
        date = today.strftime('%Y-%m-%d')
        return date, date

    # 패턴8: 어제
    if '어제' in message:
        date = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        return date, date

    # 패턴9: 며칠 전 (3일 전, 5일 전 등)
    m = re.search(r'(\d+)\s*일\s*전', message)
    if m:
        date = (today - datetime.timedelta(days=int(m.group(1)))).strftime('%Y-%m-%d')
        return date, date

    # 패턴10: 최근 N일
    m = re.search(r'최근\s*(\d+)\s*일', message)
    if m:
        start = (today - datetime.timedelta(days=int(m.group(1)) - 1)).strftime('%Y-%m-%d')
        end = today.strftime('%Y-%m-%d')
        return start, end

    # 패턴11: 이번 달
    if '이번 달' in message or '이번달' in message:
        start = today.replace(day=1).strftime('%Y-%m-%d')
        end = today.strftime('%Y-%m-%d')
        return start, end

    # 패턴12: 지난 달 / 저번 달 / 저저번 달 (N달 전)
    m = re.search(r'(저저번|저번|지난)\s*달', message)
    if m:
        months_ago = 2 if m.group(1) == '저저번' else 1
        month = today.month - months_ago
        y = today.year
        while month <= 0:
            month += 12
            y -= 1
        last_day = calendar.monthrange(y, month)[1]
        return f"{y}-{month:02d}-01", f"{y}-{month:02d}-{last_day:02d}"

    # 패턴13: 올해
    if '올해' in message:
        start = f"{year}-01-01"
        end = today.strftime('%Y-%m-%d')
        return start, end

    # 패턴14: 작년
    if '작년' in message:
        last_year = year - 1
        return f"{last_year}-01-01", f"{last_year}-12-31"

    # 패턴15: N년 전
    m = re.search(r'(\d+)\s*년\s*전', message)
    if m:
        target_year = year - int(m.group(1))
        return f"{target_year}-01-01", f"{target_year}-12-31"

    # 패턴16: N달 전
    m = re.search(r'(\d+)\s*달\s*전', message)
    if m:
        months_ago = int(m.group(1))
        month = today.month - months_ago
        y = today.year
        while month <= 0:
            month += 12
            y -= 1
        date = datetime.datetime(y, month, today.day).strftime('%Y-%m-%d')
        return date, date

    # 패턴17: N개월 전
    m = re.search(r'(\d+)\s*개월\s*전', message)
    if m:
        months_ago = int(m.group(1))
        month = today.month - months_ago
        y = today.year
        while month <= 0:
            month += 12
            y -= 1
        date = datetime.datetime(y, month, today.day).strftime('%Y-%m-%d')
        return date, date

    # 패턴18: 2026년 3월 22일
    m = re.search(r'(\d{4})\s*년\s*(\d+)\s*월\s*(\d+)\s*일', message)
    if m:
        date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return date, date

    # 패턴19: 2026년 3월 (연월 전체)
    m = re.search(r'(\d{4})\s*년\s*(\d+)\s*월', message)
    if m and not re.search(r'\d+일', message):
        y, month = int(m.group(1)), int(m.group(2))
        last_day = calendar.monthrange(y, month)[1]
        return f"{y}-{month:02d}-01", f"{y}-{month:02d}-{last_day:02d}"

    # 패턴20: 2026년 전체
    m = re.search(r'(\d{4})\s*년', message)
    if m and not re.search(r'\d+월', message):
        y = int(m.group(1))
        return f"{y}-01-01", f"{y}-12-31"

    # 패턴21: 3월 전체 (연도 없이 월만)
    m = re.search(r'(\d+)월', message)
    if m and not re.search(r'\d+일', message):
        month = int(m.group(1))
        last_day = calendar.monthrange(year, month)[1]
        return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day:02d}"

    # 패턴22: 3월 22일 (단일 날짜)
    m = re.search(r'(\d+)월\s*(\d+)일', message)
    if m:
        date = f"{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
        return date, date

    # 패턴23: 2026-03-22 (단일 날짜 숫자형)
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', message)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}", f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 패턴24: 26-03-22 또는 2026-3-22 (zero-padding 없는 경우)
    m = re.search(r'(\d{2,4})-(\d{1,2})-(\d{1,2})', message)
    if m:
        y = int(m.group(1))
        if y < 100:
            y += 2000
        date = f"{y}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return date, date

    return None

# parquet에서 날짜 범위에 맞는 이메일 필터링
def _filter_emails_by_date(paths, start_date: str, end_date: str) -> list:
    entity_path = os.path.join(paths.GRAPHRAG_ROOT, 'output', 'entities.parquet')
    df = pd.read_parquet(entity_path)

    email_df = df[df['type'].str.upper() == 'EMAIL'].copy() # 타입 필드가 EMAIL인 엔티티만 필터링

    # 날짜 범위를 datetime 객체로 변환
    start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59)

    results = []
    for _, row in email_df.iterrows():
        desc = row['description'] if pd.notna(row['description']) else ''

        # description 필드에서 "Date: YYYY-MM-DD HH:MM" 형식의 날짜 추출
        date_match = re.search(r'Date:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})', desc)
        if not date_match:
            continue # 날짜 필드 없으면 걍 넘어감

        try:
            mail_dt = datetime.datetime.strptime(date_match.group(1), '%Y-%m-%d %H:%M')
        except ValueError:
            continue # 날짜 파싱 실패하면 걍 넘어감

        if not (start_dt <= mail_dt <= end_dt):
            continue # 날짜 범위 밖이면 걍 넘어감
        
        # description에서 제목, ID, 요약을 추출함
        title_match = re.search(r'Title:\s*(.+?)\s*\|', desc)
        id_match = re.search(r'ID:\s*([a-fA-F0-9]+)', desc)
        summary_match = re.search(r'Summary:\s*(.+)', desc)

        results.append({
            'title': title_match.group(1).strip() if title_match else '(제목 없음)',
            'id': id_match.group(1).strip() if id_match else '알 수 없음',
            'date': date_match.group(1),
            'summary': summary_match.group(1).strip() if summary_match else ''
        })

    # 날짜 오름차순 정렬
    results.sort(key=lambda x: x['date'])
    return results

# 질의에서 날짜 범위 측정하여 parquet 에서 날짜 필터링 하여 llm 답변
def run_date_range_query(message: str, paths) -> str:
    date_range = _extract_date_range(message) # 질의에서 날짜 범위 추출, 날짜 패턴 없으면 None 반환
    if not date_range:
        return None  # 날짜 쿼리 아니면 graphrag로 넘긴다

    start_date, end_date = date_range
    start_time = time.time()  # 시작 시간 측정
    emails = _filter_emails_by_date(paths, start_date, end_date) # parquet에서 날짜 범위에 해당하는 이메일 필터링

    if not emails: # 해당 기간 이메일 없으면 바로 없다고 메시지 반환
        print(f'date_query execution_time : {time.time() - start_time}')
        return f"{start_date} ~ {end_date} 사이에 수신된 이메일이 없습니다."

    # 필터링된 이메일 목록 LLM에 넘길 텍스트로 변환
    lines = []
    for i, e in enumerate(emails, 1):
        lines.append(
            f"{i}. 제목: {e['title']}\n"
            f"   ID: {e['id']}\n"
            f"   날짜: {e['date']}\n"
            f"   요약: {e['summary']}"
        )
    context = "\n\n".join(lines)

    client = openai.OpenAI(api_key=os.environ.get("GRAPHRAG_API_KEY"))

    # 이메일 목록을 context로 넘겨서 LLM이 자연어로 답변 생성
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "당신은 이메일 데이터를 분석하는 어시스턴트입니다. "
                    "아래 제공된 이메일 목록을 기반으로 사용자 질문에 한국어로 답변하세요. "
                    "제공된 데이터 외의 내용은 추측하지 마세요."
                )
            },
            {
                "role": "user",
                "content": f"[이메일 목록]\n{context}\n\n[질문]\n{message}"
            }
        ],
        temperature=0.0 # 날짜 기반 질문은 창의성 필요 ㄴㄴ
    )
    print(f'date_query execution_time : {time.time() - start_time}')  # 답변 시간 출력
    print(f'date_query answer : {response.choices[0].message.content.strip()}')  # 답변 출력
    return response.choices[0].message.content.strip()