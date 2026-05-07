import os
import re
import json
import time
import datetime
import openai

def _extract_date_range_with_llm(message: str):
    today = datetime.datetime.now()
    weekday_kr = ['월', '화', '수', '목', '금', '토', '일'][today.weekday()]

    tw_start = today - datetime.timedelta(days=today.weekday())
    tw_end   = tw_start + datetime.timedelta(days=6)
    lw_start = tw_start - datetime.timedelta(weeks=1)
    lw_end   = tw_end   - datetime.timedelta(weeks=1)
    llw_start = tw_start - datetime.timedelta(weeks=2)
    llw_end   = tw_end   - datetime.timedelta(weeks=2)
    lllw_start = tw_start - datetime.timedelta(weeks=3)
    lllw_end   = tw_end   - datetime.timedelta(weeks=3)

    fmt = lambda d: d.strftime('%Y-%m-%d')
    date_context = (
        f"오늘: {fmt(today)} ({weekday_kr}요일)\n"
        f"이번 주: {fmt(tw_start)} ~ {fmt(tw_end)}\n"
        f"저번 주 / 지난 주: {fmt(lw_start)} ~ {fmt(lw_end)}\n"
        f"저저번 주: {fmt(llw_start)} ~ {fmt(llw_end)}\n"
        f"저저저번 주: {fmt(lllw_start)} ~ {fmt(lllw_end)}\n"
        f"이번 달: {fmt(today.replace(day=1))} ~ {fmt(today)}\n"
    )

    client = openai.OpenAI(api_key=os.environ.get("GRAPHRAG_API_KEY"))
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    f"{date_context}"
                    "위 날짜 기준표를 참고하여 사용자 질의가 특정 날짜나 기간을 조회하는 것이라면 set_date_range를 호출하세요. "
                    "날짜와 무관한 질의라면 아무것도 호출하지 마세요."
                )
            },
            {"role": "user", "content": message}
        ],
        tools=[{
            "type": "function",
            "function": {
                "name": "set_date_range",
                "description": "날짜/기간 기반 이메일 조회 시 호출. 날짜 무관 질의는 호출 금지.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "start_date": {"type": "string", "description": "조회 시작 날짜 (YYYY-MM-DD)"},
                        "end_date":   {"type": "string", "description": "조회 종료 날짜 (YYYY-MM-DD)"}
                    },
                    "required": ["start_date", "end_date"]
                }
            }
        }],
        tool_choice="auto",
        temperature=0.0
    )

    tool_calls = response.choices[0].message.tool_calls
    if not tool_calls:
        return None

    args = json.loads(tool_calls[0].function.arguments)
    print(f"[DEBUG] date_range extracted: {args['start_date']} ~ {args['end_date']}")
    return args["start_date"], args["end_date"]

# parquet에서 날짜 범위에 맞는 이메일 필터링
def _filter_emails_by_date(paths, start_date: str, end_date: str) -> list:
    import pandas as pd
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
    date_range = _extract_date_range_with_llm(message)
    if not date_range:
        return None  # 날짜 쿼리 아니면 graphrag로 넘긴다

    start_date, end_date = date_range
    start_time = time.time()  # 시작 시간 측정
    emails = _filter_emails_by_date(paths, start_date, end_date) # parquet에서 날짜 범위에 해당하는 이메일 필터링
    print(f"[DEBUG] filtered emails count: {len(emails)}") 

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
                    "날짜 필터링은 이미 완료되었습니다. "
                    "제공된 이메일 목록이 곧 사용자가 요청한 기간의 전체 결과입니다. "
                    "날짜 범위를 임의로 재해석하거나 변경하지 마세요. "
                    "목록이 비어있지 않다면 반드시 모든 이메일을 답변에 포함하세요."
                    "이메일 목록의 첫 번째부터 마지막까지 순서대로 전부 나열하세요."
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