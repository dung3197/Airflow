import random
import string
import pyodbc
from datetime import datetime

# ==========================
# CẤU HÌNH KẾT NỐI DATABASE
# ==========================
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=your_database;"
    "UID=sa;"
    "PWD=your_password;"
)

def get_connection():
    return pyodbc.connect(conn_str)


# ==========================
# HÀM RANDOM PHONE
# ==========================
def random_phone():
    return "09" + ''.join(random.choices("0123456789", k=8))


# ==========================
# UPDATE 10 RECORD
# ==========================
def update_random_records(log_file):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT TOP 10 cust_id FROM customer_information ORDER BY NEWID()")
    rows = cursor.fetchall()

    with open(log_file, "a", encoding="utf-8") as f:
        f.write("\n===== UPDATE LOG =====\n")

        for row in rows:
            cust_id = row[0]
            new_phone = random_phone()
            new_balance = round(random.uniform(1000, 99999), 2)
            new_time = datetime.now()

            cursor.execute("""
                UPDATE customer_information
                SET phone=?, balance=?, last_update=?
                WHERE cust_id=?
            """, (new_phone, new_balance, new_time, cust_id))

            f.write(
                f"[UPDATE] cust_id={cust_id} | "
                f"phone={new_phone} | "
                f"balance={new_balance} | "
                f"last_update={new_time}\n"
            )

    conn.commit()
    conn.close()
    print("✔ Updated 10 random records.")


# ==========================
# DELETE 2 RECORD
# ==========================
def delete_random_records(log_file):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT TOP 2 cust_id FROM customer_information ORDER BY NEWID()")
    rows = cursor.fetchall()

    with open(log_file, "a", encoding="utf-8") as f:
        f.write("\n===== DELETE LOG =====\n")

        for row in rows:
            cust_id = row[0]

            # Ghi log trước khi xóa
            f.write(f"[DELETE] cust_id={cust_id}\n")

            cursor.execute("DELETE FROM customer_information WHERE cust_id=?", cust_id)

    conn.commit()
    conn.close()
    print("✔ Deleted 2 random records.")


# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    LOG_FILE = "customer_information_log.txt"

    update_random_records(LOG_FILE)
    delete_random_records(LOG_FILE)

    print(f"✔ Log saved to: {LOG_FILE}")
