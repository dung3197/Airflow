from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import to_timestamp, lit

# ---- Spark session (add any hudi/jar config if needed on your cluster) ----
spark = (SparkSession.builder
         .appName("hudi_write_minio_example")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         # Add any Hudi/S3 jars/config as required by your environment
         .getOrCreate())

# ---- Sample schema ----
schema = StructType([
    StructField("table", StringType(), True),
    StructField("op", StringType(), True),
    StructField("op_ts", TimestampType(), True),
    StructField("current_ts", TimestampType(), True),
    StructField("pos", StringType(), True),
    StructField("primary_keys", StringType(), True),  # kept as string for simplicity
    StructField("CUST_ID", IntegerType(), True),
    StructField("NAME", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True)
])

# ---- Create DataFrames from the JSONs you provided ----
# Note: using literal values and to_timestamp to parse timestamp strings
insert_data = [
    {
        "table": "GGADMIN.CUSTOMER",
        "op": "c",
        "op_ts": "2025-09-08 13:18:08.000000",
        "current_ts": "2025-09-08 13:21:27.096000",
        "pos": "00000000040000001924",
        "primary_keys": "CUST_ID",
        "CUST_ID": 5,
        "NAME": "Le Van C",
        "CITY": "Hue",
        "STATE": "HUE"
    }
]

update_data = [
    {
        "table": "GGADMIN.CUSTOMER",
        "op": "u",
        "op_ts": "2025-09-08 13:18:20.000000",
        "current_ts": "2025-09-08 13:21:27.358000",
        "pos": "00000000040000002244",
        "primary_keys": "CUST_ID",
        "CUST_ID": 5,
        "NAME": "Le Van C",
        "CITY": "Da Nang",
        "STATE": "HUE"
    }
]

delete_data = [
    {
        "table": "GGADMIN.CUSTOMER",
        "op": "d",
        "op_ts": "2025-09-08 13:19:18.000000",
        "current_ts": "2025-09-08 13:21:27.362000",
        "pos": "00000000040000002957",
        "primary_keys": "CUST_ID",
        "CUST_ID": 5,
        "NAME": None,
        "CITY": None,
        "STATE": None
    }
]

# create DataFrames
insert_df = (spark.createDataFrame(insert_data)
             .withColumn("op_ts", to_timestamp("op_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
             .withColumn("current_ts", to_timestamp("current_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS")))

update_df = (spark.createDataFrame(update_data)
             .withColumn("op_ts", to_timestamp("op_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
             .withColumn("current_ts", to_timestamp("current_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS")))

# delete_df variable as you requested
delete_df = (spark.createDataFrame(delete_data)
             .withColumn("op_ts", to_timestamp("op_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
             .withColumn("current_ts", to_timestamp("current_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS")))

# ---- Prepare upsert DataFrame (insert + update) ----
# Hudi upsert expects the record key and (optionally) partition field and precombine field
upsert_df = insert_df.unionByName(update_df)

# Optionally show the created DataFrames
print("=== insert_df ===")
insert_df.show(truncate=False)
print("=== update_df ===")
update_df.show(truncate=False)
print("=== delete_df ===")
delete_df.show(truncate=False)

# ---- Hudi + MinIO / S3 params ----
# Replace these placeholders with your real MinIO / Hive configs
minio_endpoint = "http://minio-host:9000"           # e.g. http://minio.example.com:9000
s3_bucket = "hudi-bucket"
base_path = f"s3a://{s3_bucket}/GGADMIN.CUSTOMER"   # Hudi base path
table_name = "GGADMIN.CUSTOMER"                     # Hudi table name (can be same as source)
record_key = "CUST_ID"
precombine_field = "op_ts"                          # choose a precombine field
partition_field = ""                                # if no partitioning, keep empty string

# Spark Hadoop S3A configs for MinIO (set access/secret)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<MINIO_ACCESS_KEY>")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<MINIO_SECRET_KEY>")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# If using HTTPS, set fs.s3a.connection.ssl.enabled = true and endpoint accordingly.

# ---- Common Hudi options ----
common_hudi_opts = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.recordkey.field": record_key,
    "hoodie.datasource.write.precombine.field": precombine_field,
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",   # or COPY_ON_WRITE
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "jdbc",   # or 'hms' depending on your setup
    # Hive sync (adjust jdbcurl, username, password to your environment)
    "hoodie.datasource.hive_sync.use_jdbc":"true",
    "hoodie.datasource.hive_sync.jdbcurl": "jdbc:hive2://hive-metastore-host:10000",
    "hoodie.datasource.hive_sync.username": "<hive_user>",
    "hoodie.datasource.hive_sync.password": "<hive_password>",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": table_name,
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    # Optional: set partition field if used
}

# Remove partition setting if empty
if partition_field:
    common_hudi_opts["hoodie.datasource.write.partitionpath.field"] = partition_field
    common_hudi_opts["hoodie.datasource.hive_sync.partition_fields"] = partition_field

# ---- 1) Write upsert (insert + update) to Hudi ----
hudi_upsert_opts = dict(common_hudi_opts)
hudi_upsert_opts["hoodie.datasource.write.operation"] = "upsert"
hudi_upsert_opts["hoodie.datasource.write.payload.class"] = "org.apache.hudi.common.model.DefaultHoodieRecordPayload"

(upsert_df.write
 .format("org.apache.hudi")
 .options(**hudi_upsert_opts)
 .mode("append")
 .save(base_path))

# ---- 2) Perform delete operation using delete_df ----
# Hudi delete operation expects the recordkey field; it will delete matching records.
hudi_delete_opts = dict(common_hudi_opts)
hudi_delete_opts["hoodie.datasource.write.operation"] = "delete"

# For delete, typically you only need record key (and partition path if applicable).
# We'll select only the record key column and (optionally) partition column.
delete_keys_df = delete_df.select(record_key)  # if partition used, include partition column here

(delete_keys_df.write
 .format("org.apache.hudi")
 .options(**hudi_delete_opts)
 .mode("append")
 .save(base_path))

# ---- Notes & helpful tips ----
# 1. Ensure the Hudi Spark bundle / jars are available on the Spark classpath.
# 2. If using Hive metastore (HMS) integration rather than JDBC, set hoodie.datasource.hive_sync.mode = 'hms' and set hive.metastore.uris
# 3. If your table uses partitions, include the partition path field in upsert and delete DataFrames and add 'hoodie.datasource.write.partitionpath.field'
# 4. Hudi's delete operation requires the record key (and partition path if the table is partitioned) to identify rows to delete.
# 5. Validate the writes by reading back with spark.read.format("hudi").load(base_path + "/*") or query via Hive (after sync).

# Example: read-back quick check (optional)
# read_back_df = spark.read.format("hudi").load(base_path + "/*")
# read_back_df.filter("CUST_ID = 5").show(truncate=False)
