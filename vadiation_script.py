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







































version: 1
config:
  locale: en_US
tables:
  - table_name: customer_transactions
    row_count: 50000000
    columns:
      - column_name: customer_id
        data: fake.random_int(1, 100000000)
      - column_name: account_number
        data: fake.iban()
      - column_name: full_name
        data: fake.name()
      - column_name: email
        data: fake.email()
      - column_name: phone
        data: fake.phone_number()
      - column_name: address
        data: fake.address().replace("\n"," ")
      - column_name: date_of_birth
        data: fake.date_of_birth()
      - column_name: gender
        data: fake.random_element(['Male','Female','Other'])
      - column_name: account_type
        data: fake.random_element(['Savings','Checking','Business'])
      - column_name: account_open_date
        data: fake.date_this_decade()
      - column_name: last_transaction_date
        data: fake.date_time_this_year()
      - column_name: kyc_verified
        data: fake.boolean()
      - column_name: annual_income
        data: fake.pyfloat(left_digits=5, right_digits=2, positive=True)
      - column_name: employment_status
        data: fake.job()
      - column_name: credit_score
        data: fake.random_int(300, 850)
      - column_name: transaction_amount
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: transaction_type
        data: fake.random_element(['Deposit','Withdrawal','Transfer','Payment'])
      - column_name: merchant_name
        data: fake.company()
      - column_name: merchant_category
        data: fake.bs()
      - column_name: merchant_country
        data: fake.country()
      - column_name: transaction_currency
        data: fake.currency_code()
      - column_name: exchange_rate
        data: fake.pyfloat(left_digits=2, right_digits=4, positive=True)
      - column_name: device_type
        data: fake.random_element(['Mobile','Web','ATM','POS'])
      - column_name: login_time
        data: fake.date_time_this_month()
      - column_name: logout_time
        data: fake.date_time_this_month()
      - column_name: ip_address
        data: fake.ipv4()
      - column_name: session_duration
        data: fake.random_int(1,7200)
      - column_name: risk_score
        data: fake.pyfloat(left_digits=2, right_digits=2, positive=True)
      - column_name: loan_status
        data: fake.random_element(['Approved','Rejected','Pending'])
      - column_name: loan_amount
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: loan_type
        data: fake.random_element(['Home','Auto','Personal','Credit Line'])
      - column_name: repayment_schedule
        data: fake.random_element(['Monthly','Quarterly','Annually'])
      - column_name: missed_payments
        data: fake.random_int(0,12)
      - column_name: branch_code
        data: fake.bothify('BR###')
      - column_name: referral_code
        data: fake.bothify('REF####')
      - column_name: referred_by
        data: fake.name()
      - column_name: marketing_opt_in
        data: fake.boolean()
      - column_name: sms_alerts_enabled
        data: fake.boolean()
      - column_name: email_alerts_enabled
        data: fake.boolean()
      - column_name: overdraft_enabled
        data: fake.boolean()
      - column_name: overdraft_limit
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: last_login
        data: fake.date_time_this_year()
      - column_name: failed_login_attempts
        data: fake.random_int(0,10)
      - column_name: account_status
        data: fake.random_element(['Active','Inactive','Frozen','Closed'])
      - column_name: account_balance
        data: fake.pyfloat(left_digits=5, right_digits=2, positive=True)
      - column_name: account_tier
        data: fake.random_element(['Basic','Silver','Gold','Platinum'])
      - column_name: rewards_points
        data: fake.random_int(0,10000)
      - column_name: rewards_status
        data: fake.random_element(['Bronze','Silver','Gold'])
      - column_name: preferred_language
        data: fake.language_code()
      - column_name: number_of_dependents
        data: fake.random_int(0,5)
      - column_name: residential_status
        data: fake.random_element(['Owner','Renter','Living with Parents'])
      - column_name: housing_type
        data: fake.random_element(['Apartment','House','Townhouse'])
      - column_name: social_security_number
        data: fake.ssn()
      - column_name: passport_number
        data: fake.bothify('P########')
      - column_name: driver_license_number
        data: fake.bothify('DL#######')
      - column_name: employment_start_date
        data: fake.date_this_decade()
      - column_name: employment_end_date
        data: fake.date_this_decade()
      - column_name: tax_id
        data: fake.bothify('TIN#######')
      - column_name: salary_payment_frequency
        data: fake.random_element(['Weekly','Bi-Weekly','Monthly'])
      - column_name: salary_account
        data: fake.boolean()
      - column_name: pension_contribution
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: insurance_policy_number
        data: fake.bothify('INS#######')
      - column_name: insurance_type
        data: fake.random_element(['Health','Life','Auto','Home'])
      - column_name: insurance_expiry_date
        data: fake.future_date()
      - column_name: investment_profile
        data: fake.random_element(['Conservative','Balanced','Aggressive'])
      - column_name: crypto_wallet
        data: fake.boolean()
      - column_name: crypto_balance
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: mortgage_account
        data: fake.boolean()
      - column_name: mortgage_balance
        data: fake.pyfloat(left_digits=5, right_digits=2, positive=True)
      - column_name: rent_payment
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: monthly_expenses
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: savings_goal
        data: fake.pyfloat(left_digits=4, right_digits=2, positive=True)
      - column_name: goal_completion_percentage
        data: fake.pyfloat(left_digits=2, right_digits=2, positive=True)
      - column_name: blacklist_status
        data: fake.boolean()
      - column_name: fraud_flag
        data: fake.boolean()
      - column_name: source_system
        data: fake.random_element(['CRM','MobileApp','WebPortal','Batch'])
      - column_name: record_created_at
        data: fake.date_time_this_year()
      - column_name: record_updated_at
        data: fake.date_time_this_year()

