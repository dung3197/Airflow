from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, lit, col

# 1. Tạo Spark session
spark = (
    SparkSession.builder
    .appName("MaskCustomerCode")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

# 2. Đọc bảng Hudi gốc
source_path = "s3://your-bucket/hudi/customer_information"
df = spark.read.format("hudi").load(source_path)

# 3. Salt bí mật (bạn phải giữ riêng, không public)
SECRET_SALT = "7a9f2c1e_my_secret_salt_value"

# 4. Hash cột cust_code bằng SHA-256
df_hashed = df.withColumn(
    "cust_code",
    sha2(concat_ws("", lit(SECRET_SALT), col("cust_code")), 256)
)

# 5. Ghi ra Hudi table mới
target_path = "s3://your-bucket/hudi/customer_information_masked"

df_hashed.write.format("hudi").options(**{
    "hoodie.table.name": "customer_information_masked",
    "hoodie.datasource.write.recordkey.field": "cust_code",
    "hoodie.datasource.write.precombine.field": "update_ts",  # nếu có
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.table": "customer_information_masked"
}).mode("overwrite").save(target_path)



orders_df_masked = orders_df.withColumn(
    "cust_code",
    sha2(concat_ws("", lit(SECRET_SALT), col("cust_code")), 256)
)

joined_df = orders_df_masked.join(df_hashed, "cust_code", "inner")
