import oracledb
import random
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import uuid
import pandas as pd

# Initialize Faker for generating realistic data
fake = Faker()

# Database connection parameters (update with your Oracle DB details)
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'your_host:your_port/your_service_name'
}

# Define the table schema (50 columns)
create_table_sql = """
CREATE TABLE cash_transaction_history (
    transaction_id VARCHAR2(36),
    account_number VARCHAR2(20),
    client_name VARCHAR2(100),
    client_email VARCHAR2(100),
    transaction_date DATE,
    transaction_type VARCHAR2(20),
    amount NUMBER(15,2),
    currency VARCHAR2(3),
    stock_symbol VARCHAR2(10),
    trade_type VARCHAR2(10),
    quantity NUMBER(10),
    price_per_share NUMBER(10,2),
    total_value NUMBER(15,2),
    commission NUMBER(10,2),
    exchange VARCHAR2(20),
    settlement_date DATE,
    broker_id VARCHAR2(36),
    broker_name VARCHAR2(100),
    branch_code VARCHAR2(10),
    region VARCHAR2(50),
    country VARCHAR2(50),
    payment_method VARCHAR2(20),
    transaction_status VARCHAR2(20),
    order_id VARCHAR2(36),
    execution_time TIMESTAMP,
    clearing_house VARCHAR2(50),
    custodian VARCHAR2(50),
    portfolio_id VARCHAR2(36),
    account_type VARCHAR2(20),
    client_type VARCHAR2(20),
    tax_amount NUMBER(10,2),
    fee_type VARCHAR2(20),
    fee_amount NUMBER(10,2),
    source_system VARCHAR2(50),
    created_by VARCHAR2(50),
    created_date DATE,
    updated_by VARCHAR2(50),
    updated_date DATE,
    transaction_category VARCHAR2(20),
    sub_account VARCHAR2(20),
    client_phone VARCHAR2(20),
    client_address VARCHAR2(200),
    trade_direction VARCHAR2(10),
    market_type VARCHAR2(20),
    order_type VARCHAR2(20),
    order_status VARCHAR2(20),
    execution_venue VARCHAR2(50),
    compliance_flag VARCHAR2(10),
    risk_rating VARCHAR2(10),
    notes VARCHAR2(200),
    batch_id VARCHAR2(36)
)
"""

# Function to generate a single transaction record
def generate_transaction():
    transaction_date = fake.date_time_between(start_date='-2y', end_date='now')
    settlement_date = transaction_date + timedelta(days=random.randint(1, 3))
    return [
        str(uuid.uuid4()),  # transaction_id
        f"ACC{random.randint(100000, 999999)}",  # account_number
        fake.name(),  # client_name
        fake.email(),  # client_email
        transaction_date,  # transaction_date
        random.choice(['BUY', 'SELL', 'DIVIDEND', 'TRANSFER', 'DEPOSIT', 'WITHDRAWAL']),  # transaction_type
        round(np.random.uniform(100.00, 1000000.00), 2),  # amount
        random.choice(['USD', 'EUR', 'GBP', 'JPY']),  # currency
        random.choice(['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'FB', 'NVDA']),  # stock_symbol
        random.choice(['BUY', 'SELL']),  # trade_type
        random.randint(1, 1000),  # quantity
        round(np.random.uniform(10.00, 500.00), 2),  # price_per_share
        0.0,  # total_value (calculated later)
        round(np.random.uniform(5.00, 100.00), 2),  # commission
        random.choice(['NYSE', 'NASDAQ', 'LSE', 'TSE']),  # exchange
        settlement_date,  # settlement_date
        str(uuid.uuid4()),  # broker_id
        fake.name(),  # broker_name
        f"BR{random.randint(100, 999)}",  # branch_code
        fake.state(),  # region
        fake.country(),  # country
        random.choice(['WIRE', 'CHECK', 'ACH', 'CASH']),  # payment_method
        random.choice(['PENDING', 'COMPLETED', 'CANCELLED']),  # transaction_status
        str(uuid.uuid4()),  # order_id
        transaction_date,  # execution_time
        random.choice(['DTCC', 'Euroclear', 'Clearstream']),  # clearing_house
        random.choice(['Fidelity', 'Schwab', 'JPMorgan']),  # custodian
        str(uuid.uuid4()),  # portfolio_id
        random.choice(['INDIVIDUAL', 'JOINT', 'TRUST', 'IRA']),  # account_type
        random.choice(['RETAIL', 'INSTITUTIONAL', 'HNW']),  # client_type
        round(np.random.uniform(0.00, 5000.00), 2),  # tax_amount
        random.choice(['FLAT', 'PERCENTAGE', 'TIERED']),  # fee_type
        round(np.random.uniform(0.00, 200.00), 2),  # fee_amount
        random.choice(['TRADING_SYSTEM', 'BROKERAGE', 'MANUAL']),  # source_system
        fake.user_name(),  # created_by
        transaction_date,  # created_date
        fake.user_name(),  # updated_by
        transaction_date,  # updated_date
        random.choice(['EQUITY', 'CASH', 'DERIVATIVE']),  # transaction_category
        f"SUB{random.randint(1000, 9999)}",  # sub_account
        fake.phone_number(),  # client_phone
        fake.address().replace('\n', ', '),  # client_address
        random.choice(['LONG', 'SHORT']),  # trade_direction
        random.choice(['EQUITY', 'OPTION', 'FUTURE']),  # market_type
        random.choice(['MARKET', 'LIMIT', 'STOP']),  # order_type
        random.choice(['OPEN', 'EXECUTED', 'CANCELLED']),  # order_status
        random.choice(['DARK_POOL', 'LIT_POOL', 'OTC']),  # execution_venue
        random.choice(['YES', 'NO']),  # compliance_flag
        random.choice(['LOW', 'MEDIUM', 'HIGH']),  # risk_rating
        fake.sentence(nb_words=10),  # notes
        str(uuid.uuid4())  # batch_id
    ]

# Function to calculate total_value (quantity * price_per_share + commission)
def calculate_total_value(record):
    quantity = record[10]  # quantity
    price_per_share = record[11]  # price_per_share
    commission = record[13]  # commission
    return round(quantity * price_per_share + commission, 2)

# Function to generate and insert data in batches
def generate_and_insert_data(total_records=50_000_000, batch_size=10_000):
    try:
        # Establish Oracle connection
        connection = oracledb.connect(**db_config)
        cursor = connection.cursor()
        
        # Create table (execute once, comment out after table is created)
        # cursor.execute(create_table_sql)
        
        insert_sql = """
        INSERT INTO cash_transaction_history (
            transaction_id, account_number, client_name, client_email, transaction_date,
            transaction_type, amount, currency, stock_symbol, trade_type, quantity,
            price_per_share, total_value, commission, exchange, settlement_date,
            broker_id, broker_name, branch_code, region, country, payment_method,
            transaction_status, order_id, execution_time, clearing_house, custodian,
            portfolio_id, account_type, client_type, tax_amount, fee_type, fee_amount,
            source_system, created_by, created_date, updated_by, updated_date,
            transaction_category, sub_account, client_phone, client_address,
            trade_direction, market_type, order_type, order_status, execution_venue,
            compliance_flag, risk_rating, notes, batch_id
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15,
            :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28,
            :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41,
            :42, :43, :44, :45, :46, :47, :48, :49, :50
        )
        """
        
        records_processed = 0
        while records_processed < total_records:
            # Generate a batch of records
            batch = [generate_transaction() for _ in range(min(batch_size, total_records - records_processed))]
            
            # Calculate total_value for each record
            for record in batch:
                record[12] = calculate_total_value(record)  # Update total_value
            
            # Insert batch into Oracle
            cursor.executemany(insert_sql, batch)
            connection.commit()
            
            records_processed += len(batch)
            print(f"Inserted {records_processed:,} of {total_records:,} records")
        
        print("Data insertion completed successfully.")
        
    except oracledb.Error as e:
        print(f"Database error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

# Execute the data generation and insertion
if __name__ == "__main__":
    generate_and_insert_data()
