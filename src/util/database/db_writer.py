# gmail DB에 데이터 저장 함수

from config.db import get_db_connection

def create_user(user_account_id, started_at, ended_at, index_time,my_mail_count):
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