import pyodbc
from faker import Faker
import random
from datetime import timedelta


# ---------------------------------------------------
# CONFIG DATABASE
# ---------------------------------------------------
SERVER = "localhost"
DATABASE = "your_database"
USERNAME = "sa"
PASSWORD = "your_password"
TABLE_NAME = "customer_information"


# ---------------------------------------------------
# KẾT NỐI SQL SERVER
# ---------------------------------------------------
def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    )


fake = Faker("en_US")


# ---------------------------------------------------
# TẠO BẢNG
# ---------------------------------------------------
def create_table(cursor):
    create_table_sql = f"""
    CREATE TABLE {TABLE_NAME} (
        customer_id        INT IDENTITY(1,1) PRIMARY KEY,
        fullname           VARCHAR(100),
        gender             VARCHAR(10),
        birth_date         DATE,
        join_datetime      DATETIME,
        email              VARCHAR(150),
        phone_number       VARCHAR(20),
        address_line       VARCHAR(200),
        city               VARCHAR(100),
        country            VARCHAR(100),
        is_active          BIT,
        credit_score       INT,
        balance_amount     DECIMAL(18,2),
        last_login         DATETIME,
        created_at         DATETIME,
        updated_at         DATETIME,
        membership_level   VARCHAR(20),
        occupation         VARCHAR(100),
        notes              VARCHAR(255),
        referral_code      VARCHAR(20)
    )
    """
    cursor.execute(create_table_sql)


# ---------------------------------------------------
# HÀM SINH DỮ LIỆU CHO 1 RECORD
# ---------------------------------------------------
def random_customer_data():
    fullname = fake.name()
    gender = random.choice(["Male", "Female", "Other"])
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)

    created_at = fake.date_time_between(start_date="-3y", end_date="now")
    updated_at = created_at + timedelta(days=random.randint(0, 400))

    return (
        fullname,
        gender,
        birth_date,
        fake.date_time_between(start_date="-2y", end_date="now"),
        fake.email(),
        fake.phone_number(),
        fake.street_address(),
        fake.city(),
        fake.country(),
        random.choice([0, 1]),
        random.randint(300, 900),
        round(random.uniform(10, 50000), 2),
        fake.date_time_between(start_date="-200d", end_date="now"),
        created_at,
        updated_at,
        random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
        fake.job(),
        fake.sentence(nb_words=6),
        fake.bothify(text="REF####")
    )


# ---------------------------------------------------
# INSERT RECORDS
# ---------------------------------------------------
def insert_records(cursor, num_records: int):
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        fullname,
        gender,
        birth_date,
        join_datetime,
        email,
        phone_number,
        address_line,
        city,
        country,
        is_active,
        credit_score,
        balance_amount,
        last_login,
        created_at,
        updated_at,
        membership_level,
        occupation,
        notes,
        referral_code
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for _ in range(num_records):
        cursor.execute(insert_sql, random_customer_data())


# ---------------------------------------------------
# HÀM CHÍNH: TẠO BẢNG / GHI THÊM RECORD
# ---------------------------------------------------
def generate_customer_data(drop_table="no", num_records=1000):
    conn = get_connection()
    cursor = conn.cursor()

    # Kiểm tra bảng đã tồn tại chưa
    check_sql = f"""
    SELECT 1 FROM sysobjects WHERE name = '{TABLE_NAME}' AND xtype = 'U'
    """
    cursor.execute(check_sql)
    table_exists = cursor.fetchone() is not None

    # Nếu bảng tồn tại và được yêu cầu drop
    if table_exists and drop_table.lower() == "yes":
        print(f"Dropping existing table '{TABLE_NAME}'...")
        cursor.execute(f"DROP TABLE {TABLE_NAME}")
        conn.commit()
        table_exists = False

    # Tạo bảng nếu chưa có
    if not table_exists:
        print(f"Creating table '{TABLE_NAME}'...")
        create_table(cursor)
        conn.commit()

    # Ghi thêm dữ liệu
    print(f"Inserting {num_records} new records...")
    insert_records(cursor, num_records)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Done! {num_records} records inserted into '{TABLE_NAME}'.")


# ---------------------------------------------------
# CHẠY THỬ
# ---------------------------------------------------
if __name__ == "__main__":
    generate_customer_data(drop_table="no", num_records=1000)
