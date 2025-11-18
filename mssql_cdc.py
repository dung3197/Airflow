import pyodbc
from faker import Faker
import random
from datetime import datetime, timedelta

# ---------------------------------------------------
# CONFIG DATABASE
# ---------------------------------------------------
SERVER = "localhost"
DATABASE = "your_database"
USERNAME = "sa"
PASSWORD = "your_password"
TABLE_NAME = "customer_information"

fake = Faker("en_US")


def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    )


# ---------------------------------------------------
# LẤY DANH SÁCH ID NGẪU NHIÊN
# ---------------------------------------------------
def get_random_ids(cursor, limit):
    sql = f"""
        SELECT TOP {limit} customer_id 
        FROM {TABLE_NAME}
        ORDER BY NEWID()
    """
    cursor.execute(sql)
    return [row[0] for row in cursor.fetchall()]


# ---------------------------------------------------
# UPDATE NGẪU NHIÊN 10 RECORD
# ---------------------------------------------------
def update_random_records():
    conn = get_connection()
    cursor = conn.cursor()

    random_ids = get_random_ids(cursor, 10)

    update_sql = f"""
        UPDATE {TABLE_NAME}
        SET 
            fullname = ?,
            city = ?,
            country = ?,
            updated_at = ?
        WHERE customer_id = ?
    """

    for cid in random_ids:
        cursor.execute(
            update_sql,
            fake.name(),
            fake.city(),
            fake.country(),
            datetime.now(),
            cid
        )

    conn.commit()
    cursor.close()
    conn.close()

    print("Updated 10 random records successfully!")
    print("Updated IDs:", random_ids)


# ---------------------------------------------------
# DELETE NGẪU NHIÊN 2 RECORD
# ---------------------------------------------------
def delete_random_records():
    conn = get_connection()
    cursor = conn.cursor()

    random_ids = get_random_ids(cursor, 2)

    delete_sql = f"""
        DELETE FROM {TABLE_NAME}
        WHERE customer_id = ?
    """

    for cid in random_ids:
        cursor.execute(delete_sql, cid)

    conn.commit()
    cursor.close()
    conn.close()

    print("Deleted 2 random records successfully!")
    print("Deleted IDs:", random_ids)


# ---------------------------------------------------
# CHẠY THỬ
# ---------------------------------------------------
if __name__ == "__main__":
    update_random_records()
    delete_random_records()
