# gmail DB에 데이터 저장 함수
import os
import json
import uuid
import datetime
from config.db import get_db_connection

def get_latest_user_record(user_account_id: str):
    """
    user 테이블에서 해당 user_account_id의 가장 최근 레코드 반환
    return: dict | None
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        sql = """
            SELECT user_account_id, update_date
            FROM user
            WHERE user_account_id = %s
            ORDER BY update_date DESC
            LIMIT 1
        """
        cursor.execute(sql, (user_account_id,))
        result = cursor.fetchone()
        return result

    except Exception as e:
        print(f"[ERROR] get_latest_user_record 실패: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

def create_user(user_account_id, ended_at, index_time,my_mail_count):
    conn = get_db_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO user (user_account_id, update_date, index_time,my_mail_count)
    VALUES (%s, %s, %s, %s)
    """

    cursor.execute(sql, (
        user_account_id,
        ended_at,
        str(index_time),
         my_mail_count
    ))

    conn.commit()
    cursor.close()
    conn.close()

def save_person_stats_to_db(paths,update_date=None):
    # 나와 메일을 주고 받은 person 테이블에 삽입. 동일한 데이터는 업데이트

    if not os.path.exists(paths.MAIL_CONTACTS_PATH):
        print(f"[WARN] 파일이 없습니다: {paths.MAIL_CONTACTS_PATH}")
        return

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        if update_date is None:
            latest_user = get_latest_user_record(paths.GMAIL_ID)

            if not latest_user:
                print(f"[WARN] user 테이블에 해당 유저가 없습니다: {paths.GMAIL_ID}")
                return

            user_account_id = latest_user["user_account_id"]
            update_date = latest_user["update_date"]
        else:
            user_account_id = paths.GMAIL_ID

        # 2) JSON 읽기
        with open(paths.MAIL_CONTACTS_PATH, "r", encoding="utf-8") as f:
            stats = json.load(f)

        # 3) person 테이블 저장
        insert_sql = """
            INSERT INTO person (
                person_account_id,
                user_account_id,
                update_date,
                person_name,
                receive_mails,
                send_mails,
                friendly_mails
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                person_name = VALUES(person_name),
                receive_mails = VALUES(receive_mails),
                send_mails = VALUES(send_mails),
                friendly_mails = VALUES(friendly_mails)
        """

        inserted_count = 0

        for email, info in stats.items():
            person_name = info.get("name", "")
            receive_mails = int(info.get("received", 0))
            send_mails = int(info.get("sent", 0))
            friendly_mails = int(info.get("friendly_mail", 0))

            cursor.execute(
                insert_sql,
                (
                    email,
                    user_account_id,
                    update_date,
                    person_name,
                    receive_mails,
                    send_mails,
                    friendly_mails
                )
            )
            inserted_count += 1

        conn.commit()
        print(f"[DB] person 테이블 저장 완료: {inserted_count}건")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] save_person_stats_to_db 실패: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

def save_query_to_db(gmail_id: str, context: str, response_time: float, method: str = ""):
    latest_user = get_latest_user_record(gmail_id)
    if not latest_user:
        print(f"[WARN] save_query_to_db: user 테이블에 {gmail_id} 없음, 저장 생략")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO query (query_id, user_account_id, update_date, context, response_time, method, response_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                latest_user["user_account_id"],
                latest_user["update_date"],
                context,
                round(response_time, 5),
                method,
                datetime.datetime.now(),
            )
        )
        conn.commit()
        print(f"[DB] query 저장 완료: {gmail_id} / {response_time:.2f}s / {method}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] save_query_to_db 실패: {e}")
    finally:
        cursor.close()
        conn.close()


def save_keyword_stats_to_db(paths,update_date=None):
    """
    1. user 테이블에서 paths.GMAIL_ID에 해당하는 가장 최근 row 조회
    2. keyword json 파일 읽기
    3. keyword 테이블에 없으면 INSERT, 있으면 UPDATE
    """

    if not os.path.exists(paths.MAIL_KEYWORDS_PATH):
        print(f"[WARN] 파일이 없습니다: {paths.MAIL_KEYWORDS_PATH}")
        return

    if update_date is None:
        latest_user = get_latest_user_record(paths.GMAIL_ID)

        if not latest_user:
            print(f"[WARN] user 테이블에 해당 유저가 없습니다: {paths.GMAIL_ID}")
            return

        user_account_id = latest_user["user_account_id"]
        update_date = latest_user["update_date"]
    else:
        user_account_id = paths.GMAIL_ID

    with open(paths.MAIL_KEYWORDS_PATH, "r", encoding="utf-8") as f:
        stats = json.load(f)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        insert_sql = """
            INSERT INTO keyword (
                keyword_name,
                user_account_id,
                update_date,
                keyword_count
            )
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                keyword_count = VALUES(keyword_count)
        """

        inserted_count = 0

        keywords = stats.get("keywords", {})
        for keyword_name, keyword_count in keywords.items():
            if keyword_count < 2 : continue # 키워드 수가 2 이상인 경우만 (유효한 키워드를 얻기위함)

            cursor.execute(
                insert_sql,
                (
                    keyword_name,
                    user_account_id,
                    update_date,
                    keyword_count
                )
            )
            inserted_count += 1

        conn.commit()
        print(f"[DB] keyword 테이블 저장 완료: {inserted_count}건")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] save_keyword_stats_to_db 실패: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

def init_processed_attachments_table():
    """
    서버 시작 시 processed_attachments 테이블이 없으면 자동 생성
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_attachments (
                id               INT AUTO_INCREMENT PRIMARY KEY,
                user_account_id  VARCHAR(255) NOT NULL,
                update_date      DATETIME     NOT NULL,
                mail_id          VARCHAR(255) NOT NULL,
                filename         VARCHAR(255) NOT NULL,
                processed_at     DATETIME     NOT NULL,
                UNIQUE KEY uq_att (user_account_id, update_date, mail_id, filename),
                FOREIGN KEY (user_account_id, update_date) REFERENCES user(user_account_id, update_date)
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("[DB] processed_attachments 테이블 준비 완료")
    except Exception as e:
        print(f"[DB] processed_attachments 테이블 초기화 실패 (무시): {e}")


def filter_unprocessed_attachments(gmail_id: str, attachments: list) -> list:
    """
    이미 처리된 첨부파일 필터링
    (user_account_id, update_date, mail_id, filename) 조합으로 중복 체크
    반환: 미처리 첨부파일 리스트
    """
    if not attachments:
        return []

    latest_user = get_latest_user_record(gmail_id)
    if not latest_user:
        print(f"[WARN] filter_unprocessed_attachments: user 테이블에 {gmail_id} 없음, 전체 처리")
        return attachments

    user_account_id = latest_user["user_account_id"]
    update_date = latest_user["update_date"]

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT mail_id, filename
            FROM processed_attachments
            WHERE user_account_id = %s
              AND update_date = %s
              AND (mail_id, filename) IN ({",".join(["(%s,%s)"] * len(attachments))})
        """, [user_account_id, update_date] + [
            v for a in attachments
            for v in (a.get("mail_id", ""), a.get("name", ""))
        ])

        already_done = set((row[0], row[1]) for row in cursor.fetchall())
        cursor.close()
        conn.close()

        unprocessed = [
            a for a in attachments
            if (a.get("mail_id", ""), a.get("name", "")) not in already_done
        ]

        skipped = len(attachments) - len(unprocessed)
        if skipped > 0:
            print(f"[AttachmentFilter] 중복 제외: {skipped}개 / 처리 대상: {len(unprocessed)}개")

        return unprocessed

    except Exception as e:
        print(f"[AttachmentFilter] DB 조회 실패, 전체 처리: {e}")
        return attachments


def mark_attachments_as_processed(gmail_id: str, attachments: list):
    """
    처리 완료된 첨부파일 DB에 기록
    IGNORE: 중복 INSERT 시 오류 없이 무시
    """
    if not attachments:
        return

    latest_user = get_latest_user_record(gmail_id)
    if not latest_user:
        print(f"[WARN] mark_attachments_as_processed: user 테이블에 {gmail_id} 없음, 기록 생략")
        return

    user_account_id = latest_user["user_account_id"]
    update_date = latest_user["update_date"]

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        now = datetime.datetime.now()
        rows = [
            (user_account_id, update_date, a.get("mail_id", ""), a.get("name", ""), now)
            for a in attachments
        ]
        cursor.executemany("""
            INSERT IGNORE INTO processed_attachments
                (user_account_id, update_date, mail_id, filename, processed_at)
            VALUES (%s, %s, %s, %s, %s)
        """, rows)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[AttachmentFilter] {len(rows)}개 처리 완료 기록")
    except Exception as e:
        print(f"[AttachmentFilter] 처리 완료 기록 실패 (무시): {e}")
    