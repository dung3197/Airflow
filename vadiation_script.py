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






CREATE OR REPLACE FUNCTION get_table_row_counts()
RETURNS TABLE (table_name TEXT, row_count BIGINT) AS $$
DECLARE
    rec RECORD;
    query TEXT;
BEGIN
    -- Loop through all tables in the public schema of DB1
    FOR rec IN (
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_catalog = 'DB1'
          AND table_type = 'BASE TABLE'
    )
    LOOP
        query := format('SELECT %L AS table_name, COUNT(*) AS row_count FROM public.%I', rec.table_name, rec.table_name);
        RETURN QUERY EXECUTE query;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Call the function to get results
SELECT * FROM get_table_row_counts();





























-- First, create a pipelined function to return matching column names
CREATE OR REPLACE TYPE varchar2_table AS TABLE OF VARCHAR2(4000);
/

CREATE OR REPLACE FUNCTION get_non_null_date_columns(p_table_name IN VARCHAR2, p_owner IN VARCHAR2)
  RETURN varchar2_table PIPELINED
AS
  v_sql     VARCHAR2(1000);
  v_count   NUMBER;
BEGIN
  FOR col IN (
    SELECT column_name
    FROM all_tab_columns
    WHERE table_name = UPPER(p_table_name)
      AND owner = UPPER(p_owner)
      AND data_type = 'DATE'
  )
  LOOP
    v_sql := 'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name || 
             ' WHERE "' || col.column_name || '" IS NULL';

    EXECUTE IMMEDIATE v_sql INTO v_count;

    IF v_count = 0 THEN
      PIPE ROW(col.column_name);
    END IF;
  END LOOP;

  RETURN;
END;
/










DECLARE
  CURSOR date_columns IS
    SELECT column_name
    FROM user_tab_columns
    WHERE table_name = 'YOUR_TABLE_NAME'
      AND data_type = 'DATE';
  v_sql VARCHAR2(1000);
  v_null_count NUMBER;
BEGIN
  FOR col IN date_columns LOOP
    -- Construct dynamic SQL to count NULLs in the column
    v_sql := 'SELECT COUNT(*) FROM YOUR_TABLE_NAME WHERE ' || col.column_name || ' IS NULL';
    
    -- Execute the query
    EXECUTE IMMEDIATE v_sql INTO v_null_count;
    
    -- Output the result
    IF v_null_count = 0 THEN
      DBMS_OUTPUT.PUT_LINE('Column ' || col.column_name || ' has no NULL values.');
    ELSE
      DBMS_OUTPUT.PUT_LINE('Column ' || col.column_name || ' has ' || v_null_count || ' NULL values.');
    END IF;
  END LOOP;
END;
/







SELECT 'SELECT ''' || column_name || ''' AS column_name FROM dual WHERE NOT EXISTS (SELECT 1 FROM MY_TABLE WHERE "' || column_name || '" IS NULL) UNION ALL'
FROM all_tab_columns
WHERE table_name = 'MY_TABLE'
  AND owner = 'YOUR_SCHEMA'
  AND data_type = 'DATE';
