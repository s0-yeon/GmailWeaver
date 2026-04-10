# 웹앱 DB → 메일에서 추출한 정보 데이터 JSON
# 현재는 가라 데이터
import json
import os
from config.db import get_db_connection

# 웹앱용 가라데이터
def get_mail_stats(paths): # 메일 송수신
    try:
        with open(paths.MAIL_CONTACTS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data

    except FileNotFoundError:
        return { # 파일이 없으면 가라데이터
        "ae-best-care-market14@deals.aliexpress.com": {
            "name": "AliExpress",
            "sent": 0,
            "received": 3
        },
        "notifications@github.com": {
            "name": "uzichoi",
            "sent": 1,
            "received": 12
        },
        "inews11@seoul.go.kr": {
            "name": "서울시청",
            "sent": 0,
            "received": 2
        },
        "team@company.com": {
            "name": "프로젝트팀",
            "sent": 7,
            "received": 5
        },
        "friend123@gmail.com": {
            "name": "김민수",
            "sent": 4,
            "received": 6
        }
    }
    

def get_keyword_stats(paths): # 메일 키워드 수
    try:
        with open(paths.MAIL_KEYWORDS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        keyword_dict = data.get("keywords", {})

        # dict → 리스트 변환
        keywords_list = [
            {"word": word, "count": count}
            for word, count in keyword_dict.items()
        ]

        return {"keywords": keywords_list}

    except FileNotFoundError:
        return  {
        "keywords": [
            { "word": "회의", "count": 15 },
            { "word": "일정", "count": 2 },
            { "word": "첨부파일", "count": 9 },
            { "word": "프로젝트", "count": 58 },
            { "word": "확인", "count": 22 },
            { "word": "요청", "count": 17 },
            { "word": "보고서", "count": 11 },
            { "word": "마감", "count": 411 },
            { "word": "수정", "count": 13 },
            { "word": "공유", "count": 10 }
        ]
    }

def get_high_affinity_person_stats(paths): # 친밀한 사람 친밀도 수치
    if not os.path.exists(paths.MAIL_CONTACTS_PATH):
                return [
            {
            "email": "friend123@gmail.com",
            "name": "김민수",
            "affinity": 0.92
            },
            {
            "email": "team@company.com",
            "name": "프로젝트팀",
            "affinity": 0.78
            },
            {
            "email": "notifications@github.com",
            "name": "uzichoi",
            "affinity": 0.65
            },
            {
            "email": "inews11@seoul.go.kr",
            "name": "서울시청",
            "affinity": 0.40
            },
            {
            "email": "ae-best-care-market14@deals.aliexpress.com",
            "name": "AliExpress",
            "affinity": 0.55
            }
        ]

    with open(paths.MAIL_CONTACTS_PATH, "r", encoding="utf-8") as f:
        stats = json.load(f)

    result = []

    for email, data in stats.items():
        sent = data.get("sent", 0)
        received = data.get("received", 0)
        friendly = data.get("friendly_mail", 0)

        total = sent + received

        if total == 0:
            affinity = 0
        else:
            affinity = friendly / total

        result.append({
            "email": email,
            "name": data.get("name", ""),
            "affinity": round(affinity, 2)
        })

    # 🔥 친밀도 높은 순 정렬
    result.sort(key=lambda x: x["affinity"], reverse=True)

    return result


def get_user_rating_stats(): # 모든 유저의 Olive 만족도
    return {"total_rating" : 99}

def get_mail_sync_stats(paths): # 메일 동기화시 동기화된 메일 수, 동기화 시간
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        sql = """
        SELECT my_mail_count, index_time, update_date
        FROM user
        WHERE user_account_id = %s
        ORDER BY update_date DESC
        LIMIT 1
        """
        cursor.execute(sql, (paths.GMAIL_ID,))
        row = cursor.fetchone()

        if not row:
            return {
                "mail_count": 0,
                "sync_time": None,
                "sync_update_date": None
            }

        update_date = row["update_date"]
        if update_date is not None:
            sync_update_date = update_date.strftime("%Y-%m-%d")
        else:
            sync_update_date = None

        return {
            "mail_count": row["my_mail_count"],
            "sync_time": row["index_time"],
            "sync_update_date": sync_update_date
        }

    finally:
        cursor.close()
        conn.close()
