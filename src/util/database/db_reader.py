# 웹앱 DB → 메일에서 추출한 정보 데이터 JSON
# 현재는 가라 데이터
import json
import math
import os
import re
from datetime import date
from config.db import get_db_connection
from util.database.db_writer import get_latest_user_record

_PARQUET_TONE_SCORE = {
    "casual":        1.0,
    "transactional": 0.5,
    "formal":        0.2,
    "notification":  0.1,
    "alert":         0.1,
}
_LAMBDA = 0.01


def calculate_eis(
    user_account_id: str,
    person_account_id: str,
    update_date: str = None,
    start_date: str = None,
    end_date: str = None,
    apply_volume_correction: bool = True,
    apply_time_decay: bool = True,
) -> dict:
    """
    update_date 생략 시 DB에서 MAX(update_date)를 자동 조회.
    start_date / end_date 지정 시 mail_date 범위로 추가 필터링.

    Returns:
    {
        "R": float,
        "P": float,
        "T": float,
        "EIS": float,
        "EIS_adj": float,
        "EIS_final": float,
        "N": int,
        "S_A_to_B": int,
        "S_B_to_A": int,
        "reply_count": int,
        "t_bar": float | None,
        "delta_t_last": int | None
    }
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if update_date is None:
            latest = get_latest_user_record(user_account_id)
            update_date = latest["update_date"] if latest else None

        date_filter = ""
        params = [user_account_id, update_date, person_account_id, person_account_id]
        if start_date and end_date:
            date_filter = "AND mail_date BETWEEN %s AND %s"
            params += [start_date, end_date]

        sql = f"""
            SELECT direction, parquet_tone, llm_tone,
                   is_reply, reply_elapsed_hours, mail_date
            FROM mail
            WHERE user_account_id = %s
              AND update_date = %s
              AND (
                (direction = 'sent'     AND receiver LIKE CONCAT('%<', %s, '>%'))
                OR
                (direction = 'received' AND sender   LIKE CONCAT('%<', %s, '>%'))
              )
              {date_filter}
        """
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    # ── 1. 상호성 점수 R ──────────────────────────────────────────────────
    S_A_to_B = sum(1 for r in rows if r["direction"] == "sent")
    S_B_to_A = sum(1 for r in rows if r["direction"] == "received")
    N = S_A_to_B + S_B_to_A

    R = 0.0 if N == 0 else 1 - abs((S_A_to_B - S_B_to_A) / N)

    # ── 2. 반응성 점수 P ──────────────────────────────────────────────────
    reply_count = sum(1 for r in rows if r["is_reply"] == 1)
    elapsed = [float(r["reply_elapsed_hours"]) for r in rows if r["reply_elapsed_hours"] is not None]

    if N == 0:
        P, t_bar = 0.0, None
    else:
        reply_ratio = reply_count / N
        if elapsed:
            t_bar = sum(elapsed) / len(elapsed)
            P = reply_ratio * math.exp(-_LAMBDA * t_bar)
        else:
            t_bar = None
            P = reply_ratio

    # ── 3. 어조 점수 T ────────────────────────────────────────────────────
    if N == 0:
        T = 0.0
    else:
        tone_scores = []
        for r in rows:
            parquet_score = _PARQUET_TONE_SCORE.get(r["parquet_tone"] or "", 0.0)
            llm = r["llm_tone"]
            if llm is None:
                tone_scores.append(parquet_score)
            else:
                llm_score = 1.0 if llm == "friendly" else 0.0
                tone_scores.append((parquet_score + llm_score) / 2)
        T = sum(tone_scores) / len(tone_scores)

    # ── 4. 통합 EIS ───────────────────────────────────────────────────────
    EIS = 0.3 * R + 0.4 * P + 0.3 * T

    # ── 5. 볼륨 보정 (apply_volume_correction=False 시 생략) ──────────────
    EIS_adj = EIS * (1 - math.exp(-0.05 * N)) if apply_volume_correction else EIS

    # ── 6. 시간 감쇠 보정 (apply_time_decay=False 시 생략) ────────────────
    mail_dates = [r["mail_date"] for r in rows if r["mail_date"] is not None]
    if not apply_time_decay:
        delta_t_last = None
        EIS_final = EIS_adj
    elif not mail_dates:
        delta_t_last = None
        EIS_final = 0.0
    else:
        last_mail = max(mail_dates)
        last_date = last_mail.date() if hasattr(last_mail, "date") else last_mail
        delta_t_last = (date.today() - last_date).days
        EIS_final = EIS_adj * math.exp(-0.005 * delta_t_last)

    return {
        "R":            round(R, 6),
        "P":            round(P, 6),
        "T":            round(T, 6),
        "EIS":          round(EIS, 6),
        "EIS_adj":      round(EIS_adj, 6),
        "EIS_final":    round(EIS_final, 6),
        "N":            N,
        "S_A_to_B":     S_A_to_B,
        "S_B_to_A":     S_B_to_A,
        "reply_count":  reply_count,
        "t_bar":        round(t_bar, 4) if t_bar is not None else None,
        "delta_t_last": delta_t_last,
    }

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

# 친밀한 사람 친밀도 수치 (볼륨 보정·시간 감쇠 없이 EIS 기반)
def get_high_affinity_person_stats(paths):
    latest = get_latest_user_record(paths.GMAIL_ID)
    if not latest:
        return []
    update_date = latest["update_date"]

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT person_account_id, person_name FROM person WHERE user_account_id = %s AND update_date = %s",
            (paths.GMAIL_ID, update_date),
        )
        persons = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    result = []
    for person in persons:
        eis = calculate_eis(
            user_account_id=paths.GMAIL_ID,
            person_account_id=person["person_account_id"],
            update_date=update_date,
            apply_volume_correction=False,
            apply_time_decay=False,
        )
        result.append({
            "email": person["person_account_id"],
            "name":  person.get("person_name", ""),
            "affinity": eis["EIS_final"],
        })

    result.sort(key=lambda x: x["affinity"], reverse=True)
    return result


def get_keywords_by_person_date(gmail_id: str, person_gmail_id: str, start_date: str, end_date: str) -> list:
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT keyword_name, mail_date, SUM(daily_count) AS day_count
            FROM keyword_mail
            WHERE user_account_id   = %s
              AND person_account_id = %s
              AND mail_date BETWEEN %s AND %s
            GROUP BY keyword_name, mail_date
            ORDER BY mail_date
        """
        cursor.execute(sql, (gmail_id, person_gmail_id, start_date, end_date))
        rows = cursor.fetchall()

        keyword_map = {}
        for row in rows:
            kw = row["keyword_name"]
            date = str(row["mail_date"])
            if kw not in keyword_map:
                keyword_map[kw] = {"word": kw, "count": 0, "dates": []}
            keyword_map[kw]["count"] += row["day_count"]
            keyword_map[kw]["dates"].append(date)

        return list(keyword_map.values())
    finally:
        cursor.close()
        conn.close()


def get_user_rating_stats(): # 모든 유저의 Olive 만족도
    return {"total_rating" : 99}

def get_mail_exchange_stats(gmail_id, person_mail_id, start_date, end_date):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT MAX(update_date) AS ud FROM user WHERE user_account_id = %s", (gmail_id,))
        update_date = cursor.fetchone()["ud"]

        like_param = f"%{person_mail_id}%"
        sql = """
        SELECT
            DATE_FORMAT(mail_date, '%Y-%m') AS month,
            SUM(CASE WHEN direction = 'sent'     AND receiver LIKE %s THEN 1 ELSE 0 END) AS sent,
            SUM(CASE WHEN direction = 'received' AND sender   LIKE %s THEN 1 ELSE 0 END) AS received
        FROM mail
        WHERE user_account_id = %s
          AND update_date = %s
          AND mail_date BETWEEN %s AND %s
        GROUP BY DATE_FORMAT(mail_date, '%Y-%m')
        ORDER BY month ASC
        """
        cursor.execute(sql, (like_param, like_param, gmail_id, update_date, start_date, end_date))
        rows = cursor.fetchall()

        monthly = [
            {"month": row["month"], "sent": int(row["sent"] or 0), "received": int(row["received"] or 0)}
            for row in rows
        ]

        total_sent     = sum(m["sent"]     for m in monthly)
        total_received = sum(m["received"] for m in monthly)

        return {
            "monthly": monthly,
            "total": {"sent": total_sent, "received": total_received},
        }

    finally:
        cursor.close()
        conn.close()

def get_date_range_person_stats(gmail_id, start_date, end_date, sort_by):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT MAX(update_date) AS ud FROM user WHERE user_account_id = %s", (gmail_id,))
        update_date = cursor.fetchone()["ud"]

        direction_filter = "sent" if sort_by == "sent" else "received"
        sql = """
        SELECT sender, receiver, direction
        FROM mail
        WHERE user_account_id = %s
          AND update_date = %s
          AND mail_date BETWEEN %s AND %s
          AND direction = %s
        """
        cursor.execute(sql, (gmail_id, update_date, start_date, end_date, direction_filter))
        rows = cursor.fetchall()

        email_pattern = re.compile(r'[\w.+\-]+@[\w.\-]+')
        counts = {}

        for row in rows:
            field = row["receiver"] if sort_by == "sent" else row["sender"]
            for email in email_pattern.findall(field or ""):
                email = email.lower()
                if email == gmail_id.lower():
                    continue
                counts[email] = counts.get(email, 0) + 1

        result = [
            {"email": email, sort_by: count}
            for email, count in counts.items()
        ]
        result.sort(key=lambda x: x[sort_by], reverse=True)
        return result

    finally:
        cursor.close()
        conn.close()


def get_mail_date_range(gmail_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        sql = """
        SELECT MIN(mail_date) AS first_date, MAX(mail_date) AS last_date
        FROM mail
        WHERE user_account_id = %s
          AND update_date = (
              SELECT MAX(update_date) FROM user WHERE user_account_id = %s
          )
        """
        cursor.execute(sql, (gmail_id, gmail_id))
        row = cursor.fetchone()

        return {
            "first_date": row["first_date"].strftime("%Y-%m-%d") if row["first_date"] else None,
            "last_date":  row["last_date"].strftime("%Y-%m-%d")  if row["last_date"]  else None,
        }

    finally:
        cursor.close()
        conn.close()

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


def get_person_descriptions(gmail_id: str) -> list:
    latest_user = get_latest_user_record(gmail_id)
    if not latest_user:
        return []

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT person_account_id, person_name, description
            FROM person
            WHERE user_account_id = %s AND update_date = %s
              AND description IS NOT NULL AND description != ''
        """, (latest_user["user_account_id"], latest_user["update_date"]))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
