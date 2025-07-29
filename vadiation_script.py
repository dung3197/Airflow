import boto3
import fastavro
import pandas as pd
from io import BytesIO
from great_expectations.dataset import PandasDataset

# Connect to MinIO
s3 = boto3.client(
    's3',
    aws_access_key_id='minio',
    aws_secret_access_key='minio123',
    endpoint_url='http://localhost:9000'
)

# List Avro files in partitioned structure
bucket = 'commerce'
prefix = 'debezium.commerce.products/'
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

# Read Avro files from specific date partitions
records = []
target_date = '2025-07-10'  # Example: Validate specific date
for obj in response.get('Contents', []):
    if obj['Key'].startswith(f'{prefix}date={target_date}/') and obj['Key'].endswith('.avro'):
        avro_file = s3.get_object(Bucket=bucket, Key=obj['Key'])
        avro_bytes = avro_file['Body'].read()
        avro_reader = fastavro.reader(BytesIO(avro_bytes))
        records.extend(list(avro_reader))

# Convert to Pandas DataFrame
df = pd.DataFrame(records)

# Create Great Expectations dataset
ge_df = PandasDataset(df)

# Define expectations
ge_df.expect_column_values_to_not_be_null("id")
ge_df.expect_table_row_count_to_be_greater_than(0)
ge_df.expect_column_values_to_match_regex("name", r"^[A-Za-z\s]+$")
ge_df.expect_column_values_to_be_unique("id")  # Check for duplicates
ge_df.save_expectation_suite("expectations_avro.json")

# Run validation
results = ge_df.validate()
print(results)

# Compare with source database (e.g., MySQL)
import mysql.connector
conn = mysql.connector.connect(user='user', password='pass', host='localhost', database='commerce')
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM products WHERE DATE(updated_at) = '2025-07-10'")
db_count = cursor.fetchone()[0]
minio_count = len(df)
print(f"Database count for {target_date}: {db_count}, MinIO count: {minio_count}")
cursor.close()
conn.close()




















data_source:
  type: spark
  connection:
    master: local  # Adjust to your Spark cluster URL if not local
    app_name: soda-spark
  options:
    spark.sql.warehouse.dir: /tmp/spark-warehouse
    spark.hadoop.fs.s3a.access.key: minio         # Your MinIO access key
    spark.hadoop.fs.s3a.secret.key: minio123      # Your MinIO secret key
    spark.hadoop.fs.s3a.endpoint: http://localhost:9000  # Your MinIO endpoint
    spark.hadoop.fs.s3a.path.style.access: true   # Enable path-style access for MinIO








scan:
  data_source: spark
  sql: |
    SELECT * FROM avro.`s3a://commerce/debezium.commerce.products/date=2025-07-10/*.avro`

checks:
  - row_count > 0                       # Ensure there are records
  - missing_count(id) = 0               # Check for missing IDs
  - duplicate_count(id) = 0             # Check for duplicate IDs
  - schema:                             # Validate schema for 'id'
      name: id
      type: integer
  - schema:                             # Validate schema for 'name'
      name: name
      type: string



scan:
  data_source: spark
  sql: |
    SELECT * FROM avro.`s3a://commerce/debezium.commerce.products/date=${date}/*.avro`










{
  "version": "1.0",
  "entries": [
    {
      "dataPath": "CDC_OGG/Category1",
      "name": "Category1",
      "type": "table",
      "format": "avro",
      "partitions": [
        {
          "name": "date",
          "type": "string"
        }
      ]
    },
    {
      "dataPath": "CDC_OGG/Category2",
      "name": "Category2",
      "type": "table",
      "format": "avro",
      "partitions": [
        {
          "name": "date",
          "type": "string"
        }
      ]
    }
  ]
}







import pandas as pd

# Step 1: Read files
excel_df = pd.read_excel('your_excel_file.xlsx')
csv_df = pd.read_csv('your_csv_file.csv')

# Step 2: Get only the UserId columns (dropna just in case)
excel_user_ids = set(excel_df['UserId'].dropna())
csv_user_ids = set(csv_df['UserId'].dropna())

# Step 3: Find unmatched UserIds
only_in_excel = excel_user_ids - csv_user_ids
only_in_csv = csv_user_ids - excel_user_ids

# Step 4: Print or save the results
print("UserIds only in Excel file:")
print(only_in_excel)

print("\nUserIds only in CSV file:")
print(only_in_csv)

# Optional: Create DataFrames from unmatched sets
df_only_in_excel = excel_df[excel_df['UserId'].isin(only_in_excel)]
df_only_in_csv = csv_df[csv_df['UserId'].isin(only_in_csv)]

# Save to files if needed
# df_only_in_excel.to_csv('only_in_excel.csv', index=False)
# df_only_in_csv.to_csv('only_in_csv.csv', index=False)










DO $$
DECLARE
    rec RECORD;
    query TEXT;
    result RECORD;
BEGIN
    -- Create a temporary table to store results
    CREATE TEMP TABLE row_counts (table_name TEXT, row_count BIGINT);

    -- Loop through all tables in the public schema
    FOR rec IN (
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_catalog = 'DB1'
          AND table_type = 'BASE TABLE'
    )
    LOOP
        query := format('SELECT %L AS table_name, COUNT(*) AS row_count FROM public.%I', rec.table_name, rec.table_name);
        EXECUTE query INTO result;
        INSERT INTO row_counts (table_name, row_count) VALUES (result.table_name, result.row_count);
    END LOOP;

    -- Select the results
    SELECT * FROM row_counts;

    -- Drop the temporary table
    DROP TABLE row_counts;
END $$;


















-- Create a temporary table to store results
CREATE TEMP TABLE row_counts (table_name TEXT, row_count BIGINT);

-- Populate the temporary table
DO $$
DECLARE
    rec RECORD;
    query TEXT;
BEGIN
    -- Loop through all tables in the public schema
    FOR rec IN (
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_catalog = 'DB1'
          AND table_type = 'BASE TABLE'
    )
    LOOP
        query := format('INSERT INTO row_counts (table_name, row_count) SELECT %L, COUNT(*) FROM public.%I', rec.table_name, rec.table_name);
        EXECUTE query;
    END LOOP;
END $$;

-- Query the results
SELECT * FROM row_counts;

-- Clean up
DROP TABLE row_counts;





SELECT 'COL1' AS column_name
FROM dual
WHERE NOT EXISTS (
  SELECT 1 FROM MY_TABLE WHERE COL1 IS NULL
)
UNION ALL
SELECT 'COL2'
FROM dual
WHERE NOT EXISTS (
  SELECT 1 FROM MY_TABLE WHERE COL2 IS NULL
)
UNION ALL
SELECT 'COL3'
FROM dual
WHERE NOT EXISTS (
  SELECT 1 FROM MY_TABLE WHERE COL3 IS NULL
);







SELECT column_name, 
       'SELECT ''' || column_name || ''' AS column_name FROM dual WHERE NOT EXISTS (SELECT 1 FROM ' || table_name || ' WHERE ' || column_name || ' IS NULL FETCH FIRST 1 ROW ONLY)' AS sql_to_run
FROM user_tab_columns
WHERE table_name = 'EMPLOYEES'
  AND data_type = 'DATE';







































table_name: customer_transactions
num_rows: 50000000
columns:
  - { name: customer_id, type: int, faker: random_int }
  - { name: account_number, type: string, faker: iban }
  - { name: full_name, type: string, faker: name }
  - { name: email, type: string, faker: email }
  - { name: phone, type: string, faker: phone_number }
  - { name: address, type: string, faker: address }
  - { name: date_of_birth, type: date, faker: date_of_birth }
  - { name: gender, type: string, faker: random_element, options: { elements: [Male, Female, Other] } }
  - { name: marital_status, type: string, faker: random_element, options: { elements: [Single, Married, Divorced, Widowed] } }
  - { name: nationality, type: string, faker: country }
  - { name: employment_status, type: string, faker: job }
  - { name: employer_name, type: string, faker: company }
  - { name: annual_income, type: float, faker: pyfloat }
  - { name: credit_score, type: int, faker: random_int, options: { min: 300, max: 850 } }
  - { name: kyc_verified, type: bool, faker: boolean }
  - { name: social_security_number, type: string, faker: ssn }
  - { name: passport_number, type: string, faker: bothify, options: { text: "??######" } }
  - { name: driver_license_number, type: string, faker: bothify, options: { text: "DL#########" } }
  - { name: account_type, type: string, faker: random_element, options: { elements: [Savings, Checking, Business, Fixed Deposit] } }
  - { name: account_open_date, type: datetime, faker: date_this_decade }
  - { name: account_status, type: string, faker: random_element, options: { elements: [Active, Suspended, Closed] } }
  - { name: branch_code, type: string, faker: bothify, options: { text: "BR###" } }
  - { name: account_balance, type: float, faker: pyfloat }
  - { name: overdraft_enabled, type: bool, faker: boolean }
  - { name: overdraft_limit, type: float, faker: pyfloat }
  - { name: last_transaction_date, type: datetime, faker: date_time_this_year }
  - { name: transaction_amount, type: float, faker: pyfloat }
  - { name: transaction_type, type: string, faker: random_element, options: { elements: [Deposit, Withdrawal, Transfer, Payment] } }
  - { name: merchant_name, type: string, faker: company }
  - { name: merchant_category, type: string, faker: random_element, options: { elements: [Retail, Travel, Dining, Health, Utilities, Education] } }
  - { name: merchant_country, type: string, faker: country }
  - { name: transaction_currency, type: string, faker: currency_code }
  - { name: exchange_rate, type: float, faker: pyfloat }
  - { name: transaction_status, type: string, faker: random_element, options: { elements: [Success, Failed, Pending] } }
  - { name: risk_score, type: float, faker: pyfloat }
  - { name: fraud_flag, type: bool, faker: boolean }
  - { name: login_time, type: datetime, faker: date_time_this_year }
  - { name: logout_time, type: datetime, faker: date_time_this_year }
  - { name: session_duration, type: int, faker: random_int, options: { min: 1, max: 7200 } }
  - { name: device_type, type: string, faker: random_element, options: { elements: [Mobile, Desktop, Tablet] } }
  - { name: ip_address, type: string, faker: ipv4 }
  - { name: referral_code, type: string, faker: bothify, options: { text: "REF####" } }
  - { name: referred_by, type: string, faker: name }
  - { name: marketing_opt_in, type: bool, faker: boolean }
  - { name: sms_alerts_enabled, type: bool, faker: boolean }
  - { name: email_alerts_enabled, type: bool, faker: boolean }
  - { name: rewards_points, type: int, faker: random_int, options: { min: 0, max: 100000 } }
  - { name: rewards_status, type: string, faker: random_element, options: { elements: [Silver, Gold, Platinum, Diamond] } }
  - { name: preferred_language, type: string, faker: language_name }
  - { name: education_level, type: string, faker: random_element, options: { elements: [High School, Bachelor, Master, PhD] } }
  - { name: university_attended, type: string, faker: company }
  - { name: graduation_year, type: int, faker: year }
  - { name: housing_type, type: string, faker: random_element, options: { elements: [Owned, Rented, Mortgaged, Company Provided] } }
  - { name: number_of_dependents, type: int, faker: random_int, options: { min: 0, max: 5 } }
  - { name: residential_status, type: string, faker: random_element, options: { elements: [Resident, Non-Resident] } }
  - { name: utility_provider, type: string, faker: company }
  - { name: tax_id, type: string, faker: bothify, options: { text: "TAX########" } }
  - { name: salary_account, type: bool, faker: boolean }
  - { name: salary_payment_frequency, type: string, faker: random_element, options: { elements: [Monthly, Bi-Weekly, Weekly] } }
  - { name: employment_start_date, type: date, faker: date_this_decade }
  - { name: employment_end_date, type: date, faker: date_this_decade }
  - { name: loan_status, type: string, faker: random_element, options: { elements: [Active, Closed, Defaulted] } }
  - { name: loan_amount, type: float, faker: pyfloat }
  - { name: loan_type, type: string, faker: random_element, options: [Personal, Home, Auto, Education, Credit Card] }
  - { name: repayment_schedule, type: string, faker: random_element, options: [Monthly, Quarterly, Bi-Annually, Annually] }
  - { name: missed_payments, type: int, faker: random_int, options: { min: 0, max: 12 } }
  - { name: pension_contribution, type: float, faker: pyfloat }
  - { name: insurance_policy_number, type: string, faker: bothify, options: { text: "INS##########" } }
  - { name: insurance_type, type: string, faker: random_element, options: [Health, Life, Auto, Travel] }
  - { name: insurance_expiry_date, type: date, faker: date_this_decade }
  - { name: crypto_wallet, type: bool, faker: boolean }
  - { name: crypto_balance, type: float, faker: pyfloat }
  - { name: savings_goal, type: float, faker: pyfloat }
  - { name: goal_completion_percentage, type: float, faker: pyfloat }
  - { name: rent_payment, type: float, faker: pyfloat }
  - { name: monthly_expenses, type: float, faker: pyfloat }
  - { name: source_system, type: string, faker: random_element, options: [CoreBanking, MobileApp, WebPortal, ATM] }
  - { name: blacklist_status, type: bool, faker: boolean }
  - { name: record_created_at, type: datetime, faker: date_time_this_year }
  - { name: record_updated_at, type: datetime, faker: date_time_this_year }

