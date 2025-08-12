import oracledb
import random
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import uuid
import os

# Initialize Faker for generating realistic data
fake = Faker()

# Database connection parameters (update with your Oracle DB details)
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'your_host:your_port/your_service_name',
    'timeout': 30  # Connection timeout in seconds
}

# Table creation SQL with named constraints
create_table_sql = """
CREATE TABLE customer_information (
    customer_id VARCHAR2(36) CONSTRAINT pk_customer_id PRIMARY KEY,
    account_number VARCHAR2(20) CONSTRAINT uk_account_number UNIQUE,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    middle_name VARCHAR2(50),
    email VARCHAR2(100),
    phone_number VARCHAR2(20),
    mobile_number VARCHAR2(20),
    address_line1 VARCHAR2(100),
    address_line2 VARCHAR2(100),
    city VARCHAR2(50),
    state VARCHAR2(50),
    postal_code VARCHAR2(20),
    country VARCHAR2(50),
    date_of_birth DATE,
    ssn VARCHAR2(20),
    nationality VARCHAR2(50),
    occupation VARCHAR2(50),
    employer_name VARCHAR2(100),
    employer_phone VARCHAR2(20),
    annual_income NUMBER(15,2),
    net_worth NUMBER(15,2),
    credit_score NUMBER(3),
    account_type VARCHAR2(20),
    account_status VARCHAR2(20),
    account_open_date DATE,
    account_close_date DATE,
    portfolio_id VARCHAR2(36),
    broker_id VARCHAR2(36),
    branch_code VARCHAR2(10),
    region VARCHAR2(50),
    customer_segment VARCHAR2(20),
    risk_tolerance VARCHAR2(20),
    investment_objective VARCHAR2(50),
    investment_experience VARCHAR2(20),
    income_source VARCHAR2(50),
    tax_id VARCHAR2(20),
    tax_residency VARCHAR2(50),
    kyc_status VARCHAR2(20),
    aml_flag VARCHAR2(10),
    compliance_status VARCHAR2(20),
    account_balance NUMBER(15,2),
    margin_balance NUMBER(15,2),
    available_cash NUMBER(15,2),
    last_transaction_date DATE,
    last_login_date TIMESTAMP,
    preferred_currency VARCHAR2(3),
    communication_preference VARCHAR2(20),
    language_preference VARCHAR2(20),
    time_zone VARCHAR2(50),
    customer_rating VARCHAR2(10),
    referral_source VARCHAR2(50),
    marketing_consent VARCHAR2(10),
    account_manager VARCHAR2(100),
    account_manager_id VARCHAR2(36),
    last_review_date DATE,
    next_review_date DATE,
    customer_notes VARCHAR2(200),
    created_by VARCHAR2(50),
    created_date DATE,
    updated_by VARCHAR2(50),
    updated_date DATE,
    client_type VARCHAR2(20),
    investment_horizon VARCHAR2(20),
    preferred_contact_method VARCHAR2(20),
    alternate_email VARCHAR2(100),
    emergency_contact_name VARCHAR2(100),
    emergency_contact_phone VARCHAR2(20),
    relationship_manager VARCHAR2(100),
    risk_score NUMBER(3),
    compliance_notes VARCHAR2(200),
    batch_id VARCHAR2(36)
)
"""

# Creating indexes for frequently queried columns
create_indexes_sql = """
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_customer_email ON customer_information(email)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_customer_ssn ON customer_information(ssn)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_customer_portfolio_id ON customer_information(portfolio_id)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_customer_broker_id ON customer_information(broker_id)';
END;
"""

# Function to generate a single customer record
def generate_customer():
    open_date = fake.date_time_between(start_date='-5y', end_date='now')
    dob = fake.date_between(start_date='-80y', end_date='-18y')
    last_tx_date = open_date + timedelta(days=random.randint(1, 365*2))
    last_login = open_date + timedelta(days=random.randint(1, 365*2))
    last_review = open_date + timedelta(days=random.randint(1, 365))
    next_review = last_review + timedelta(days=random.randint(90, 365))
    close_date = None if random.random() > 0.1 else open_date + timedelta(days=random.randint(366, 365*5))
    return [
        str(uuid.uuid4()),  # customer_id
        f"ACC{random.randint(100000000000, 999999999999)}",  # account_number (12 digits for more uniqueness)
        fake.first_name(),  # first_name
        fake.last_name(),  # last_name
        fake.first_name() if random.random() > 0.5 else None,  # middle_name
        fake.email(),  # email
        fake.phone_number(),  # phone_number
        fake.phone_number(),  # mobile_number
        fake.street_address(),  # address_line1
        fake.secondary_address() if random.random() > 0.5 else None,  # address_line2
        fake.city(),  # city
        fake.state(),  # state
        fake.postcode(),  # postal_code
        fake.country(),  # country
        dob,  # date_of_birth
        fake.ssn(),  # ssn
        fake.country(),  # nationality
        fake.job(),  # occupation
        fake.company(),  # employer_name
        fake.phone_number(),  # employer_phone
        round(np.random.uniform(20000.00, 500000.00), 2),  # annual_income
        round(np.random.uniform(50000.00, 10000000.00), 2),  # net_worth
        random.randint(300, 850),  # credit_score
        random.choice(['INDIVIDUAL', 'JOINT', 'TRUST', 'IRA']),  # account_type
        random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED', 'CLOSED']),  # account_status
        open_date,  # account_open_date
        close_date,  # account_close_date
        str(uuid.uuid4()),  # portfolio_id
        str(uuid.uuid4()),  # broker_id
        f"BR{random.randint(100, 999)}",  # branch_code
        fake.state(),  # region
        random.choice(['RETAIL', 'HNW', 'INSTITUTIONAL']),  # customer_segment
        random.choice(['LOW', 'MEDIUM', 'HIGH']),  # risk_tolerance
        random.choice(['GROWTH', 'INCOME', 'PRESERVATION', 'SPECULATIVE']),  # investment_objective
        random.choice(['NONE', 'LIMITED', 'GOOD', 'EXTENSIVE']),  # investment_experience
        random.choice(['EMPLOYMENT', 'INVESTMENT', 'INHERITANCE', 'BUSINESS']),  # income_source
        fake.ssn(),  # tax_id
        fake.country(),  # tax_residency
        random.choice(['VERIFIED', 'PENDING', 'REJECTED']),  # kyc_status
        random.choice(['YES', 'NO']),  # aml_flag
        random.choice(['COMPLIANT', 'NON_COMPLIANT', 'UNDER_REVIEW']),  # compliance_status
        round(np.random.uniform(0.00, 1000000.00), 2),  # account_balance
        round(np.random.uniform(0.00, 500000.00), 2),  # margin_balance
        round(np.random.uniform(0.00, 1000000.00), 2),  # available_cash
        last_tx_date,  # last_transaction_date
        last_login,  # last_login_date (TIMESTAMP)
        random.choice(['USD', 'EUR', 'GBP', 'JPY']),  # preferred_currency
        random.choice(['EMAIL', 'PHONE', 'MAIL']),  # communication_preference
        random.choice(['EN', 'ES', 'FR', 'DE']),  # language_preference
        fake.timezone(),  # time_zone
        random.choice(['A', 'B', 'C', 'D', 'F']),  # customer_rating
        random.choice(['WEB', 'REFERRAL', 'AD', 'WALK_IN']),  # referral_source
        random.choice(['YES', 'NO']),  # marketing_consent
        fake.name(),  # account_manager
        str(uuid.uuid4()),  # account_manager_id
        last_review,  # last_review_date
        next_review,  # next_review_date
        fake.sentence(nb_words=10),  # customer_notes
        fake.user_name(),  # created_by
        open_date,  # created_date
        fake.user_name(),  # updated_by
        open_date,  # updated_date
        random.choice(['RETAIL', 'INSTITUTIONAL', 'HNW']),  # client_type
        random.choice(['SHORT_TERM', 'MEDIUM_TERM', 'LONG_TERM']),  # investment_horizon
        random.choice(['EMAIL', 'PHONE', 'SMS']),  # preferred_contact_method
        fake.email() if random.random() > 0.5 else None,  # alternate_email
        fake.name(),  # emergency_contact_name
        fake.phone_number(),  # emergency_contact_phone
        fake.name(),  # relationship_manager
        random.randint(1, 10),  # risk_score
        fake.sentence(nb_words=10),  # compliance_notes
        str(uuid.uuid4())  # batch_id
    ]

# Function to generate and insert data in batches
def generate_and_insert_data(total_records=1_000_000, batch_size=10_000, create_if_not_exists=True, truncate_if_exists=True):
    # Create log file with timestamp
    log_file = f"customer_insert_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    try:
        # Establish Oracle connection
        print("Connecting to database...")
        connection = oracledb.connect(**db_config)
        cursor = connection.cursor()
        
        with open(log_file, 'w') as f:
            f.write(f"Customer Insert Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Check if table exists
            cursor.execute("SELECT COUNT(*) FROM user_tables WHERE table_name = 'CUSTOMER_INFORMATION'")
            table_exists = cursor.fetchone()[0] > 0
            f.write(f"Table exists: {table_exists}\n")
            print(f"Table exists: {table_exists}")
            
            if not table_exists and create_if_not_exists:
                f.write("Creating table...\n")
                print("Creating table...")
                cursor.execute(create_table_sql)
                cursor.execute(create_indexes_sql)
                connection.commit()
                f.write("Table and indexes created.\n")
                print("Table and indexes created.")
            elif table_exists and truncate_if_exists:
                f.write("Truncating table...\n")
                print("Truncating table...")
                cursor.execute("TRUNCATE TABLE customer_information")
                connection.commit()
                f.write("Table truncated.\n")
                print("Table truncated.")
            
            # Check for table locks
            print("Checking for table locks...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM v$lock l
                JOIN dba_objects o ON l.id1 = o.object_id
                WHERE o.object_name = 'CUSTOMER_INFORMATION'
            """)
            lock_count = cursor.fetchone()[0]
            if lock_count > 0:
                f.write(f"WARNING: Found {lock_count} locks on customer_information table\n")
                print(f"WARNING: Found {lock_count} locks on customer_information table")

            insert_sql = """
            INSERT INTO customer_information (
                customer_id, account_number, first_name, last_name, middle_name, email, phone_number,
                mobile_number, address_line1, address_line2, city, state, postal_code, country,
                date_of_birth, ssn, nationality, occupation, employer_name, employer_phone,
                annual_income, net_worth, credit_score, account_type, account_status,
                account_open_date, account_close_date, portfolio_id, broker_id, branch_code,
                region, customer_segment, risk_tolerance, investment_objective, investment_experience,
                income_source, tax_id, tax_residency, kyc_status, aml_flag, compliance_status,
                account_balance, margin_balance, available_cash, last_transaction_date, last_login_date,
                preferred_currency, communication_preference, language_preference, time_zone,
                customer_rating, referral_source, marketing_consent, account_manager, account_manager_id,
                last_review_date, next_review_date, customer_notes, created_by, created_date,
                updated_by, updated_date, client_type, investment_horizon, preferred_contact_method,
                alternate_email, emergency_contact_name, emergency_contact_phone, relationship_manager,
                risk_score, compliance_notes, batch_id
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
                :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38,
                :39, :40, :41, :42, :43, :44, :45, :46, :47, :48, :49, :50, :51, :52, :53, :54, :55, :56,
                :57, :58, :59, :60, :61, :62, :63, :64, :65, :66, :67, :68, :69, :70, :71, :72
            )
            """
            
            records_processed = 0
            skipped_records = 0
            regenerated_account = 0
            regenerated_uuid = 0
            
            while records_processed < total_records:
                batch = [generate_customer() for _ in range(min(batch_size, total_records - records_processed))]
                
                # Insert batch into Oracle
                try:
                    print(f"Inserting batch of {len(batch)} records...")
                    cursor.executemany(insert_sql, batch)
                    connection.commit()
                    records_processed += len(batch)
                    f.write(f"Inserted {records_processed:,} of {total_records:,} records\n")
                    print(f"Inserted {records_processed:,} of {total_records:,} records")
                except oracledb.Error as e:
                    error, = e.args
                    if error.code == 1:  # ORA-00001: unique constraint violated
                        f.write(f"Unique constraint error in batch: {error.message}\n")
                        print(f"Unique constraint error in batch: {error.message}")
                        # Retry individual records
                        for record in batch:
                            max_retries = 5  # Limit retries to avoid infinite loop
                            retries = 0
                            while retries < max_retries:
                                try:
                                    cursor.execute(insert_sql, record)
                                    connection.commit()
                                    records_processed += 1
                                    f.write(f"Inserted record: customer_id={record[0]}\n")
                                    print(f"Inserted record: customer_id={record[0]}")
                                    break
                                except oracledb.Error as e2:
                                    error2, = e2.args
                                    if error2.code == 1:
                                        retries += 1
                                        if 'UK_ACCOUNT_NUMBER' in error2.message.upper():
                                            regenerated_account += 1
                                            record[1] = f"ACC{random.randint(100000000000, 999999999999)}"  # Regenerate account_number
                                            f.write(f"Regenerated account_number for customer_id={record[0]} (attempt {retries})\n")
                                            print(f"Regenerated account_number for customer_id={record[0]} (attempt {retries})")
                                        elif 'PK_CUSTOMER_ID' in error2.message.upper():
                                            regenerated_uuid += 1
                                            record[0] = str(uuid.uuid4())  # Regenerate customer_id
                                            f.write(f"Regenerated customer_id (attempt {retries}): new={record[0]}\n")
                                            print(f"Regenerated customer_id (attempt {retries}): new={record[0]}")
                                        else:
                                            f.write(f"Unknown unique constraint: {error2.message}\n")
                                            print(f"Unknown unique constraint: {error2.message}")
                                            # Regenerate both
                                            record[0] = str(uuid.uuid4())
                                            record[1] = f"ACC{random.randint(100000000000, 999999999999)}"
                                    else:
                                        f.write(f"Other database error for record: {error2.message}\n")
                                        print(f"Other database error for record: {error2.message}")
                                        raise
                            if retries == max_retries:
                                f.write(f"Skipped record after max retries: customer_id={record[0]}\n")
                                print(f"Skipped record after max retries: customer_id={record[0]}")
                                skipped_records += 1
                    else:
                        f.write(f"Database error: {error.message}\n")
                        print(f"Database error: {error.message}")
                        raise
                
            f.write(f"\nData insertion completed successfully.\n")
            f.write(f"Total records inserted: {records_processed:,}\n")
            f.write(f"Total records skipped due to duplicates: {skipped_records}\n")
            f.write(f"Total account_numbers regenerated: {regenerated_account}\n")
            f.write(f"Total UUIDs regenerated: {regenerated_uuid}\n")
            print(f"Data insertion completed successfully. Total records inserted: {records_processed:,}, Skipped: {skipped_records}, Regenerated account: {regenerated_account}, Regenerated UUID: {regenerated_uuid}")
        
    except oracledb.Error as e:
        error, = e.args
        error_msg = f"Database error: {error.code} - {error.message}\n"
        print(error_msg)
        with open(log_file, 'a') as f:
            f.write(error_msg)
        connection.rollback()
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}\n"
        print(error_msg)
        with open(log_file, 'a') as f:
            f.write(error_msg)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print(f"Database connection closed. Log file created: {log_file}")
        with open(log_file, 'a') as f:
            f.write("Database connection closed\n")

# Execute the data generation and insertion
if __name__ == "__main__":
    generate_and_insert_data(create_if_not_exists=True, truncate_if_exists=True)
