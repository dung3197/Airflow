import pyodbc
from faker import Faker
import random
from datetime import datetime
import json
import os

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
# LOGGING
# ---------------------------------------------------
def append_log(file_name, data):
    if os.path.exists(file_name):
        with open(file_name, "r", encoding="utf-8") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(data)

    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=4, ensure_ascii=False)


# ---------------------------------------------------
# LẤY DANH SÁCH ID NGẪU NHIÊN
# ---------------------------------------------------
def get_random_ids(cursor, limit):
    sql = f"SELECT TOP {limit} customer_id FROM {TABLE_NAME} ORDER BY NEWID()"
    cursor.execute(sql)
    return [row[0] for row in cursor.fetchall()]


# ---------------------------------------------------
# LẤY FULL RECORD THEO ID
# ---------------------------------------------------
def get_record_by_id(cursor, cid):
    cursor.execute(
        f"SELECT * FROM {TABLE_NAME} WHERE customer_id = ?", cid
    )
    row = cursor.fetchone()
    if not row:
        return None

    columns = [column[0] for column in cursor.description]
    return dict(zip(columns, row))


# ---------------------------------------------------
# UPDATE NGẪU NHIÊN 10 RECORD + LOG
# ---------------------------------------------------
def update_random_records():
    conn = get_connection()
    cursor = conn.cursor()

    random_ids = get_random_ids(cursor, 10)

    update_sql = f"""
        UPDATE {TABLE_NAME}
        SET fullname = ?, city = ?, country = ?, updated_at = ?
        WHERE customer_id = ?
    """

    for cid in random_ids:
        before = get_record_by_id(cursor, cid)

        new_fullname = fake.name()
        new_city = fake.city()
        new_country = fake.country()
        new_updated_at = datetime.now()

        cursor.execute(
            update_sql,
            new_fullname,
            new_city,
            new_country,
            new_updated_at,
            cid
        )

        after = before.copy()
        after["fullname"] = new_fullname
        after["city"] = new_city
        after["country"] = new_country
        after["updated_at"] = new_updated_at

        # Ghi log chi tiết
        append_log("update_log.json", {
            "record_id": cid,
            "before": before,
            "after": after,
            "update_time": str(datetime.now())
        })

    conn.commit()
    cursor.close()
    conn.close()

    print("Updated 10 records + logged to update_log.json")


# ---------------------------------------------------
# DELETE NGẪU NHIÊN 2 RECORD + LOG
# ---------------------------------------------------
def delete_random_records():
    conn = get_connection()
    cursor = conn.cursor()

    random_ids = get_random_ids(cursor, 2)

    delete_sql = f"DELETE FROM {TABLE_NAME} WHERE customer_id = ?"

    for cid in random_ids:
        before = get_record_by_id(cursor, cid)

        cursor.execute(delete_sql, cid)

        # Ghi log chi tiết
        append_log("delete_log.json", {
            "record_id": cid,
            "deleted_data": before,
            "delete_time": str(datetime.now())
        })

    conn.commit()
    cursor.close()
    conn.close()

    print("Deleted 2 records + logged to delete_log.json")


# ---------------------------------------------------
# CHẠY THỬ
# ---------------------------------------------------
if __name__ == "__main__":
    update_random_records()
    delete_random_records()
