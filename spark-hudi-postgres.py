from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Write Hudi Table To Postgres")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

hudi_path = "s3a://warehouse/hudi/customer"

postgres_url = "jdbc:postgresql://postgres-host:5432/mydb"

postgres_properties = {
    "user": "postgres_user",
    "password": "postgres_password",
    "driver": "org.postgresql.Driver",
    "batchsize": "10000"
}

target_table = "public.customer"

df = (
    spark.read
    .format("hudi")
    .load(hudi_path)
)

# Remove Hudi metadata columns before writing to PostgreSQL
hudi_metadata_cols = [
    "_hoodie_commit_time",
    "_hoodie_commit_seqno",
    "_hoodie_record_key",
    "_hoodie_partition_path",
    "_hoodie_file_name"
]

clean_df = df.drop(*[c for c in hudi_metadata_cols if c in df.columns])

(
    clean_df.write
    .mode("append")
    .jdbc(
        url=postgres_url,
        table=target_table,
        properties=postgres_properties
    )
)
