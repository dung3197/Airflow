from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, rand
import uuid

spark = (
    SparkSession.builder
    .appName("HudiUUIDSync")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hudi.bootstrap.enable", "true")
    .getOrCreate()
)

# ===================== #
# 1. Đọc bảng gốc
# ===================== #
source_path = "s3a://bucket/customer_information"
target_path = "s3a://bucket/customer_information_uuid"

df_source = spark.read.format("hudi").load(source_path)

# ===================== #
# 2. Kiểm tra bảng đích đã tồn tại chưa
# ===================== #
try:
    df_target = spark.read.format("hudi").load(target_path)
    table_exists = True
except Exception:
    table_exists = False

# ===================== #
# 3. UDF sinh UUID
# ===================== #
@udf("string")
def gen_uuid():
    return str(uuid.uuid4())

# ===================== #
# 4. Xử lý lần đầu (init)
# ===================== #
if not table_exists:
    print("===> Initializing new UUID table with 50% sample...")

    # Lấy 50% random record
    df_half = df_source.orderBy(rand()).limit(int(df_source.count() * 0.5))

    df_with_uuid = df_half.withColumn("uuid", gen_uuid())

    (
        df_with_uuid.write
        .format("hudi")
        .option("hoodie.table.name", "customer_information_uuid")
        .option("hoodie.datasource.write.recordkey.field", "cust_id")
        .option("hoodie.datasource.write.precombine.field", "cust_id")
        .option("hoodie.datasource.write.operation", "insert")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .mode("overwrite")
        .save(target_path)
    )

else:
    print("===> Incremental sync mode...")

    # Lấy cust_id từ bảng đích
    df_existing_ids = df_target.select("cust_id").distinct()

    # Lọc record mới xuất hiện trong bảng gốc
    df_new_records = df_source.join(df_existing_ids, "cust_id", "leftanti")

    if df_new_records.count() > 0:
        df_new_with_uuid = df_new_records.withColumn("uuid", gen_uuid())

        (
            df_new_with_uuid.write
            .format("hudi")
            .option("hoodie.table.name", "customer_information_uuid")
            .option("hoodie.datasource.write.recordkey.field", "cust_id")
            .option("hoodie.datasource.write.precombine.field", "cust_id")
            .option("hoodie.datasource.write.operation", "upsert")
            .mode("append")
            .save(target_path)
        )

        print(f"===> Added {df_new_records.count()} new records to UUID table.")
    else:
        print("===> No new records to add.")
