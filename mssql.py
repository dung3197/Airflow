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
        random.choice
