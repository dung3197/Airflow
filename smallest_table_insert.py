import oracledb
import random
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import uuid
import pandas as pd  # Optional, if needed for any data handling

# Initialize Faker for generating realistic data
fake = Faker()

# Database connection parameters (update with your Oracle DB details)
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'your_host:your_port/your_service_name'
}

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
        f"ACC{random.randint(100000, 999999)}",  # account_number
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
def generate_and_insert_data(total_records=1_000_000, batch_size=10_000):
    try:
        # Establish Oracle connection
        connection = oracledb.connect(**db_config)
        cursor = connection.cursor()
        
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
        while records_processed < total_records:
            # Generate a batch of records
            batch = [generate_customer() for _ in range(min(batch_size, total_records - records_processed))]
            
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
