```python
import oracledb
import random
from faker import Faker
from datetime import datetime, timedelta
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Faker for realistic data
fake = Faker()

# Database connection configuration
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'localhost:1521/your_service_name'  # Replace with your Oracle DSN
}

# Table creation SQL
create_table_sql = """
CREATE TABLE stock_transactions (
    transaction_id NUMBER NOT NULL,
    trade_timestamp TIMESTAMP NOT NULL,
    trader_id VARCHAR2(20),
    trader_name VARCHAR2(100),
    trader_email VARCHAR2(100),
    trader_department VARCHAR2(50),
    stock_symbol VARCHAR2(10),
    stock_name VARCHAR2(100),
    exchange_code VARCHAR2(10),
    market_type VARCHAR2(20),
    quantity NUMBER,
    price_per_share NUMBER(10,2),
    total_amount NUMBER(15,2),
    transaction_type VARCHAR2(20),
    order_type VARCHAR2(20),
    broker_id VARCHAR2(20),
    broker_name VARCHAR2(100),
    commission_fee NUMBER(10,2),
    tax_amount NUMBER(10,2),
    settlement_date DATE,
    account_number VARCHAR2(30),
    portfolio_id VARCHAR2(20),
    trade_status VARCHAR2(20),
    execution_time NUMBER(10,2),
    order_id VARCHAR2(30),
    client_id VARCHAR2(20),
    client_name VARCHAR2(100),
    client_type VARCHAR2(20),
    currency_code VARCHAR2(3),
    exchange_rate NUMBER(10,4),
    sector VARCHAR2(50),
    industry VARCHAR2(50),
    country_code VARCHAR2(3),
    region VARCHAR2(50),
    regulatory_flag VARCHAR2(10),
    compliance_status VARCHAR2(20),
    risk_level VARCHAR2(10),
    margin_required NUMBER(10,2),
    margin_available NUMBER(10,2),
    leverage_ratio NUMBER(5,2),
    trade_source VARCHAR2(20),
    trade_channel VARCHAR2(20),
    device_type VARCHAR2(20),
    ip_address VARCHAR2(15),
    transaction_notes VARCHAR2(255),
    counterparty_id VARCHAR2(20),
    counterparty_name VARCHAR2(100),
    clearing_house VARCHAR2(50),
    clearing_status VARCHAR2(20),
    custody_bank VARCHAR2(50),
    custody_account VARCHAR2(30),
    trade_category VARCHAR2(20),
    trade_priority VARCHAR2(10),
    execution_venue VARCHAR2(50),
    market_maker_id VARCHAR2(20),
    market_maker_name VARCHAR2(100),
    liquidity_score NUMBER(5,2),
    volatility_score NUMBER(5,2),
    trade_execution_score NUMBER(5,2),
    portfolio_balance NUMBER(15,2),
    portfolio_type VARCHAR2(20),
    investment_strategy VARCHAR2(50),
    trade_purpose VARCHAR2(50),
    order_timestamp TIMESTAMP,
    order_expiry_date DATE,
    trade_settlement_type VARCHAR2(20),
    payment_method VARCHAR2(20),
    payment_status VARCHAR2(20),
    tax_jurisdiction VARCHAR2(50),
    tax_withholding NUMBER(10,2),
    dividend_flag VARCHAR2(10),
    dividend_amount NUMBER(10,2),
    corporate_action VARCHAR2(50),
    corporate_action_flag VARCHAR2(10),
    analyst_rating VARCHAR2(20),
    analyst_target_price NUMBER(10,2),
    market_cap NUMBER(15,2),
    pe_ratio NUMBER(10,2),
    eps NUMBER(10,2),
    dividend_yield NUMBER(5,2),
    beta NUMBER(5,2),
    short_interest NUMBER(10,2),
    institutional_ownership NUMBER(5,2),
    insider_trading_flag VARCHAR2(10),
    insider_trading_date DATE,
    trade_restriction VARCHAR2(20),
    trade_restriction_reason VARCHAR2(100),
    audit_trail_id VARCHAR2(30),
    audit_timestamp TIMESTAMP,
    data_source VARCHAR2(20),
    data_quality_score NUMBER(5,2),
    last_updated_by VARCHAR2(100),
    last_update_timestamp TIMESTAMP,
    batch_id VARCHAR2(30),
    batch_process_date DATE,
    etl_job_id VARCHAR2(30),
    etl_load_timestamp TIMESTAMP,
    custom_field_1 VARCHAR2(50),
    custom_field_2 VARCHAR2(50),
    custom_field_3 NUMBER(10,2),
    CONSTRAINT pk_stock_transactions PRIMARY KEY (transaction_id, trade_timestamp)
) TABLESPACE your_tablespace
"""

# Function to generate a single row of synthetic data
def generate_transaction_row(transaction_id, base_date):
    trade_date = base_date + timedelta(days=random.randint(-365, 0))
    return (
        transaction_id,  # transaction_id
        trade_date,  # trade_timestamp
        f"TR{str(random.randint(1000, 9999))}",  # trader_id
        fake.name(),  # trader_name
        fake.email(),  # trader_email
        random.choice(['Equities', 'Derivatives', 'Fixed Income']),  # trader_department
        random.choice(['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']),  # stock_symbol
        random.choice(['Apple Inc.', 'Alphabet Inc.', 'Microsoft Corp.', 'Tesla Inc.', 'Amazon.com Inc.']),  # stock_name
        random.choice(['NYSE', 'NASDAQ', 'LSE']),  # exchange_code
        random.choice(['Primary', 'Secondary', 'OTC']),  # market_type
        random.randint(1, 10000),  # quantity
        round(random.uniform(10, 500), 2),  # price_per_share
        0,  # total_amount (calculated later)
        random.choice(['Buy', 'Sell', 'Short']),  # transaction_type
        random.choice(['Market', 'Limit', 'Stop']),  # order_type
        f"BR{str(random.randint(100, 999))}",  # broker_id
        fake.company(),  # broker_name
        round(random.uniform(0, 100), 2),  # commission_fee
        round(random.uniform(0, 50), 2),  # tax_amount
        trade_date + timedelta(days=2),  # settlement_date
        f"ACC{str(random.randint(100000, 999999))}",  # account_number
        f"PF{str(random.randint(1000, 9999))}",  # portfolio_id
        random.choice(['Pending', 'Completed', 'Cancelled']),  # trade_status
        round(random.uniform(0.1, 5.0), 2),  # execution_time
        f"ORD{str(random.randint(100000, 999999))}",  # order_id
        f"CL{str(random.randint(1000, 9999))}",  # client_id
        fake.company(),  # client_name
        random.choice(['Individual', 'Institutional', 'Corporate']),  # client_type
        random.choice(['USD', 'EUR', 'GBP']),  # currency_code
        round(random.uniform(0.8, 1.2), 4),  # exchange_rate
        random.choice(['Technology', 'Finance', 'Healthcare']),  # sector
        random.choice(['Software', 'Banking', 'Pharma']),  # industry
        random.choice(['US', 'UK', 'DE']),  # country_code
        random.choice(['North America', 'Europe', 'Asia']),  # region
        random.choice(['Yes', 'No征服No', 'N/A']),  # regulatory_flag
        random.choice(['Compliant', 'Pending', 'Non-Compliant']),  # compliance_status
        random.choice(['Low', 'Medium', 'High']),  # risk_level
        round(random.uniform(0, 1000), 2),  # margin_required
        round(random.uniform(1000, 10000), 2),  # margin_available
        round(random.uniform(1, 5), 2),  # leverage_ratio
        random.choice(['Web', 'API', 'Manual']),  # trade_source
        random.choice(['Online', 'Broker', 'Phone']),  # trade_channel
        random.choice(['Desktop', 'Mobile', 'Tablet']),  # device_type
        fake.ipv4(),  # ip_address
        fake.sentence(nb_words=10),  # transaction_notes
        f"CP{str(random.randint(1000, 9999))}",  # counterparty_id
        fake.company(),  # counterparty_name
        random.choice(['DTCC', 'Euroclear', 'Clearstream']),  # clearing_house
        random.choice(['Cleared', 'Pending', 'Failed']),  # clearing_status
        fake.company(),  # custody_bank
        f"CUS{str(random.randint(100000, 999999))}",  # custody_account
        random.choice(['Equity', 'Option', 'Futures']),  # trade_category
        random.choice(['High', 'Medium', 'Low']),  # trade_priority
        random.choice(['NYSE', 'NASDAQ', 'DarkPool']),  # execution_venue
        f"MM{str(random.randint(100, 999))}",  # market_maker_id
        fake.company(),  # market_maker_name
        round(random.uniform(0, 100), 2),  # liquidity_score
        round(random.uniform(0, 100), 2),  # volatility_score
        round(random.uniform(0, 100), 2),  # trade_execution_score
        round(random.uniform(10000, 1000000), 2),  # portfolio_balance
        random.choice(['Growth', 'Value', 'Balanced']),  # portfolio_type
        random.choice(['Long-Term', 'Short-Term', 'Hedging']),  # investment_strategy
        random.choice(['Investment', 'Speculation', 'Arbitrage']),  # trade_purpose
        trade_date - timedelta(hours=random.randint(1, 24)),  # order_timestamp
        trade_date + timedelta(days=30),  # order_expiry_date
        random.choice(['T+2', 'T+1', 'T+0']),  # trade_settlement_type
        random.choice(['Wire', 'ACH', 'Check']),  # payment_method
        random.choice(['Paid', 'Pending', 'Failed']),  # payment_status
        random.choice(['US', 'UK', 'EU']),  # tax_jurisdiction
        round(random.uniform(0, 100), 2),  # tax_withholding
        random.choice(['Yes', 'No']),  # dividend_flag
        round(random.uniform(0, 50), 2),  # dividend_amount
        random.choice(['None', 'Split', 'Merger']),  # corporate_action
        random.choice(['Yes', 'No']),  # corporate_action_flag
        random.choice(['Buy', 'Sell', 'Hold']),  # analyst_rating
        round(random.uniform(50, 1000), 2),  # analyst_target_price
        round(random.uniform(1000000000, 1000000000000), 2),  # market_cap
        round(random.uniform(10, 50), 2),  # pe_ratio
        round(random.uniform(1, 10), 2),  # eps
        round(random.uniform(0, 5), 2),  # dividend_yield
        round(random.uniform(0.5, 2), 2),  # beta
        round(random.uniform(0, 1000000), 2),  # short_interest
        round(random.uniform(0, 100), 2),  # institutional_ownership
        random.choice(['Yes', 'No']),  # insider_trading_flag
        trade_date - timedelta(days=random.randint(0, 30)),  # insider_trading_date
        random.choice(['None', 'Restricted', 'Blackout']),  # trade_restriction
        fake.sentence(nb_words=5),  # trade_restriction_reason
        f"AUD{str(random.randint(100000, 999999))}",  # audit_trail_id
        trade_date,  # audit_timestamp
        random.choice(['Internal', 'External', 'API']),  # data_source
        round(random.uniform(0, 100), 2),  # data_quality_score
        fake.name(),  # last_updated_by
        trade_date,  # last_update_timestamp
        f"BAT{str(random.randint(1000, 9999))}",  # batch_id
        trade_date,  # batch_process_date
        f"ETL{str(random.randint(1000, 9999))}",  # etl_job_id
        trade_date,  # etl_load_timestamp
        fake.word(),  # custom_field_1
        fake.word(),  # custom_field_2
        round(random.uniform(0, 1000), 2)  # custom_field_3
    )

# Function to calculate total_amount
def calculate_total_amount(row):
    quantity = row[10]  # quantity
    price_per_share = row[11]  # price_per_share
    return (quantity * price_per_share,)

# Main function to create table and insert data
def main():
    try:
        # Connect to Oracle Database
        logging.info("Connecting to Oracle Database...")
        connection = oracledb.connect(
            user=db_config['user'],
            password=db_config['password'],
            dsn=db_config['dsn']
        )
        cursor = connection.cursor()

        # Create table
        logging.info("Creating table stock_transactions...")
        cursor.execute("DROP TABLE stock_transactions PURGE", ignore_errors=True)
        cursor.execute(create_table_sql)

        # Optimize cursor settings for bulk insert
        cursor.arraysize = 10000
        cursor.prefetchrows = 10001  # Minimize round-trips

        # Generate and insert data in batches
        batch_size = 10000
        total_records = 100000000  # 100 million
        num_batches = total_records // batch_size
        base_date = datetime.now()

        logging.info(f"Inserting {total_records} records in batches of {batch_size}...")
        start_time = time.time()

        for batch_num in range(num_batches):
            rows = []
            for i in range(batch_size):
                transaction_id = batch_num * batch_size + i + 1
                row = generate_transaction_row(transaction_id, base_date)
                row = row[:12] + calculate_total_amount(row) + row[13:]  # Calculate total_amount
                rows.append(row)

            # Bulk insert
            cursor.executemany(
                """INSERT /*+ APPEND */ INTO stock_transactions (
                    transaction_id, trade_timestamp, trader_id, trader_name, trader_email,
                    trader_department, stock_symbol, stock_name, exchange_code, market_type,
                    quantity, price_per_share, total_amount, transaction_type, order_type,
                    broker_id, broker_name, commission_fee, tax_amount, settlement_date,
                    account_number, portfolio_id, trade_status, execution_time, order_id,
                    client_id, client_name, client_type, currency_code, exchange_rate,
                    sector, industry, country_code, region, regulatory_flag, compliance_status,
                    risk_level, margin_required, margin_available, leverage_ratio, trade_source,
                    trade_channel, device_type, ip_address, transaction_notes, counterparty_id,
                    counterparty_name, clearing_house, clearing_status, custody_bank,
                    custody_account, trade_category, trade_priority, execution_venue,
                    market_maker_id, market_maker_name, liquidity_score, volatility_score,
                    trade_execution_score, portfolio_balance, portfolio_type, investment_strategy,
                    trade_purpose, order_timestamp, order_expiry_date, trade_settlement_type,
                    payment_method, payment_status, tax_jurisdiction, tax_withholding,
                    dividend_flag, dividend_amount, corporate_action, corporate_action_flag,
                    analyst_rating, analyst_target_price, market_cap, pe_ratio, eps,
                    dividend_yield, beta, short_interest, institutional_ownership,
                    insider_trading_flag, insider_trading_date, trade_restriction,
                    trade_restriction_reason, audit_trail_id, audit_timestamp, data_source,
                    data_quality_score, last_updated_by, last_update_timestamp, batch_id,
                    batch_process_date, etl_job_id, etl_load_timestamp, custom_field_1,
                    custom_field_2, custom_field_3
                ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15,
                    :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29,
                    :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43,
                    :44, :45, :46, :47, :48, :49, :50, :51, :52, :53, :54, :55, :56, :57,
                    :58, :59, :60, :61, :62, :63, :64, :65, :66, :67, :68, :69, :70, :71,
                    :72, :73, :74, :75, :76, :77, :78, :79, :80, :81, :82, :83, :84, :85,
                    :86, :87, :88, :89, :90, :91, :92, :93, :94, :95, :96, :97, :98, :99, :100
                )""",
                rows
            )
            connection.commit()
            logging.info(f"Inserted batch {batch_num + 1}/{num_batches} ({(batch_num + 1) * batch_size} records)")

        # Handle remaining records
        remaining_records = total_records % batch_size
        if remaining_records > 0:
            rows = []
            for i in range(remaining_records):
                transaction_id = num_batches * batch_size + i + 1
                row = generate_transaction_row(transaction_id, base_date)
                row = row[:12] + calculate_total_amount(row) + row[13:]  # Calculate total_amount
                rows.append(row)
            cursor.executemany(
                """INSERT /*+ APPEND */ INTO stock_transactions (
                    transaction_id, trade_timestamp, trader_id, trader_name, trader_email,
                    trader_department, stock_symbol, stock_name, exchange_code, market_type,
                    quantity, price_per_share, total_amount, transaction_type, order_type,
                    broker_id, broker_name, commission_fee, tax_amount, settlement_date,
                    account_number, portfolio_id, trade_status, execution_time, order_id,
                    client_id, client_name, client_type, currency_code, exchange_rate,
                    sector, industry, country_code, region, regulatory_flag, compliance_status,
                    risk_level, margin_required, margin_available, leverage_ratio, trade_source,
                    trade_channel, device_type, ip_address, transaction_notes, counterparty_id,
                    counterparty_name, clearing_house, clearing_status, custody_bank,
                    custody_account, trade_category, trade_priority, execution_venue,
                    market_maker_id, market_maker_name, liquidity_score, volatility_score,
                    trade_execution_score, portfolio_balance, portfolio_type, investment_strategy,
                    trade_purpose, order_timestamp, order_expiry_date, trade_settlement_type,
                    payment_method, payment_status, tax_jurisdiction, tax_withholding,
                    dividend_flag, dividend_amount, corporate_action, corporate_action_flag,
                    analyst_rating, analyst_target_price, market_cap, pe_ratio, eps,
                    dividend_yield, beta, short_interest, institutional_ownership,
                    insider_trading_flag, insider_trading_date, trade_restriction,
                    trade_restriction_reason, audit_trail_id, audit_timestamp, data_source,
                    data_quality_score, last_updated_by, last_update_timestamp, batch_id,
                    batch_process_date, etl_job_id, etl_load_timestamp, custom_field_1,
                    custom_field_2, custom_field_3
                ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15,
                    :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29,
                    :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43,
                    :44, :45, :46, :47, :48, :49, :50, :51, :52, :53, :54, :55, :56, :57,
                    :58, :59, :60, :61, :62, :63, :64, :65, :66, :67, :68, :69, :70, :71,
                    :72, :73, :74, :75, :76, :77, :78, :79, :80, :81, :82, :83, :84, :85,
                    :86, :87, :88, :89, :90, :91, :92, :93, :94, :95, :96, :97, :98, :99, :100
                )""",
                rows
            )
            connection.commit()
            logging.info(f"Inserted remaining {remaining_records} records")

        end_time = time.time()
        logging.info(f"Data insertion completed in {end_time - start_time:.2f} seconds")

        # Create additional indexes (optional, post-insertion for performance)
        logging.info("Creating additional indexes...")
        cursor.execute("CREATE INDEX idx_stock_symbol ON stock_transactions(stock_symbol) TABLESPACE your_tablespace")
        cursor.execute("CREATE INDEX idx_trader_id ON stock_transactions(trader_id) TABLESPACE your_tablespace")
        connection.commit()

    except oracledb.Error as e:
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error: {e}")
        raise
    finally:
        cursor.close()
        connection.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()
```
