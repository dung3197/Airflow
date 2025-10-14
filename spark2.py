# Giả sử bạn đang chạy PySpark (SparkSession: spark) với Hudi connector trên classpath.
# Thay đổi các path và table names theo môi trường của bạn.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, IntegerType
import uuid
import math

# UDF tạo UUID4 (string)
def _uuid4():
    return str(uuid.uuid4())

# Nếu dùng UDF trong pyspark:
from pyspark.sql.functions import udf
uuid4_udf = udf(_uuid4, StringType())

def generate_or_update_uuid_mapping(
    spark,
    source_hudi_path,        # path đến thư mục/hudi table source (customer_information)
    mapping_table_path,      # path lưu mapping table (Hudi table path hoặc parquet folder)
    mapping_table_is_hudi=True,
    sample_fraction=0.5,     # mong muốn là 50% của tổng số record
    mapping_hudi_table_name="customer_uuid_map",  # tên table khi ghi Hudi (khi mapping_table_is_hudi=True)
    hudi_options=None        # dict các option Hudi khi write (nếu cần)
):
    """
    Hàm đọc source Hudi, đọc mapping (nếu có) và cập nhật mapping sao cho có uuid cho ~ sample_fraction của records.
    - Không tạo uuid mới cho các cust_id đã có.
    - Khi cần tăng số uuid (do source tăng), sẽ chọn ngẫu nhiên cust_id chưa có và gán uuid.
    - Trả về dataframe mapping hiện tại (cust_id, uuid)
    """

    # 1) đọc bảng source Hudi
    # Nếu Hudi connector: spark.read.format("hudi").load(path)
    try:
        source_df = spark.read.format("hudi").load(source_hudi_path)
    except Exception as e:
        # fallback: thư mục Parquet/Delta/... try read as parquet
        source_df = spark.read.load(source_hudi_path)

    # giữ chỉ cust_id (giả sử tên cột là 'cust_id' và là 5 chữ số)
    source_ids = source_df.select(F.col("cust_id")).distinct()

    # 2) đọc mapping hiện có (nếu có)
    import os
    fs = None
    try:
        # Hudi read
        if mapping_table_is_hudi:
            existing_map = spark.read.format("hudi").load(mapping_table_path)
        else:
            existing_map = spark.read.load(mapping_table_path)
        # bảo đảm có đúng schema cust_id, uuid
        existing_map = existing_map.select(F.col("cust_id"), F.col("uuid"))
    except Exception as e:
        # chưa có mapping -> tạo empty DF schema tương ứng
        existing_map = spark.createDataFrame([], schema="cust_id string, uuid string")

    # 3) thống kê để biết cần tạo bao nhiêu uuid mới
    total_source = source_ids.count()
    target_total = int(math.floor(sample_fraction * total_source))
    existing_count = existing_map.select("cust_id").distinct().count()
    need_to_assign = target_total - existing_count

    # Nếu không cần thêm, trả về mapping hiện tại (idempotent)
    if need_to_assign <= 0:
        # Nếu mapping không có đủ rows (ví dụ có duplicate uuid), cũng có thể tiến hành check uniqueness
        # kiểm tra duplicate uuid trong existing_map
        dup_uuids = existing_map.groupBy("uuid").count().filter("count > 1")
        if dup_uuids.count() > 0:
            # rất hiếm, nhưng ta sẽ xử lý: remap các cust_id bị duplicate (regenerate uuid cho những cust_id sau dòng đầu)
            dup_uuid_values = [row["uuid"] for row in dup_uuids.collect()]
            # cho đơn giản ở đây: báo log / raise cảnh báo
            print("Warning: duplicate uuids found in existing mapping. Please inspect.")
        return existing_map

    # 4) tìm cust_id chưa có mapping
    missing_ids = source_ids.join(existing_map.select("cust_id").withColumnRenamed("cust_id","m_cust_id"),
                                   source_ids.cust_id == F.col("m_cust_id"),
                                   how="leftanti").select("cust_id")
    missing_count = missing_ids.count()
    if missing_count == 0:
        # không có cust mới nhưng need_to_assign > 0 (hiếm: có thể existing_count < target_total vì previous failures)
        # trong trường hợp này, không thể tạo uuid cho cust không tồn tại => trả về mapping.
        print("No missing cust_id to assign (all source ids already in mapping).")
        return existing_map

    # 5) sampling: chọn need_to_assign cust_id từ missing_ids ngẫu nhiên
    # nếu missing_count < need_to_assign thì chỉ tạo cho tất cả missing
    n_to_sample = min(need_to_assign, missing_count)
    # Thực hiện sampling: tạo cột rand và lấy top n
    sampled_new = (missing_ids
                   .withColumn("_rand", F.rand())  # hàm rand() phân phối đồng đều
                   .orderBy(F.col("_rand"))
                   .limit(n_to_sample)
                   .select("cust_id"))

    # 6) tạo uuid cho sampled_new
    # vì UDF uuid4_udf chạy worker-side, rất nhanh; khả năng trùng cực kỳ nhỏ.
    new_with_uuid = sampled_new.withColumn("uuid", uuid4_udf())

    # 6.1 Kiểm tra trùng uuid giữa new_with_uuid và existing_map (rất hiếm)
    # Nếu phát hiện trùng uuid (cùng uuid đã có trong existing), ta sẽ regenerate cho các rows đó.
    existing_uuids = existing_map.select("uuid").filter(F.col("uuid").isNotNull())
    # join để tìm collision
    collisions = new_with_uuid.join(existing_uuids, on="uuid", how="inner").select(new_with_uuid["cust_id"], new_with_uuid["uuid"])
    collision_count = collisions.count()
    if collision_count > 0:
        # regenerate uuid cho những cust_id va chạm
        collision_ids = [r["cust_id"] for r in collisions.collect()]
        # lập DF cho những id này và regen uuid cho từng id cho tới khi không còn collision
        # vòng lặp vì collision hiếm nên hiệu quả
        to_fix = spark.createDataFrame([(cid,) for cid in collision_ids], ["cust_id"])
        fixed = to_fix.withColumn("uuid", uuid4_udf())
        # lặp kiểm tra nữa (sử dụng join với existing_uuids) - simple loop but collisions extremely unlikely
        while True:
            coll2 = fixed.join(existing_uuids, on="uuid", how="inner").count()
            if coll2 == 0:
                break
            # regen lại cho những còn colliding
            colliding_ids = [r["cust_id"] for r in fixed.join(existing_uuids, on="uuid", how="inner").select("cust_id").collect()]
            fixed = spark.createDataFrame([(cid,) for cid in colliding_ids], ["cust_id"]).withColumn("uuid", uuid4_udf())

        # replace collision rows in new_with_uuid
        non_collision_new = new_with_uuid.join(fixed.select("cust_id"), on="cust_id", how="leftanti")
        new_with_uuid = non_collision_new.unionByName(fixed.select("cust_id","uuid"))

    # 7) hợp nhất existing_map và new_with_uuid -> mapping_updated
    mapping_updated = existing_map.unionByName(new_with_uuid).dropDuplicates(["cust_id"])

    # 8) final sanity: kiểm tra duplicate uuid nội bộ mapping_updated
    dup_uuids_final = mapping_updated.groupBy("uuid").count().filter("count > 1")
    if dup_uuids_final.count() > 0:
        # cực kỳ hiếm; log/raise để người vận hành can thiệp
        print("ERROR: duplicate uuid found after assignment. Aborting write for safety.")
        raise RuntimeError("Duplicate uuid detected in mapping_updated; manual inspection required.")

    # 9) ghi/upsert mapping_updated trở lại storage (Hudi recommended)
    if mapping_table_is_hudi:
        # Một ví dụ options cơ bản cho Hudi upsert
        default_hudi_opts = {
            "hoodie.table.name": mapping_hudi_table_name,
            "hoodie.datasource.write.recordkey.field": "cust_id",
            "hoodie.datasource.write.precombine.field": "cust_id",  # cust_id unique nên ok
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.table.type": "MERGE_ON_READ",
            "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload"
        }
        write_opts = default_hudi_opts.copy()
        if hudi_options:
            write_opts.update(hudi_options)

        (mapping_updated
            .write.format("hudi")
            .options(**write_opts)
            .mode("append")  # append sẽ upsert khi key trùng
            .save(mapping_table_path)
        )
    else:
        # Ghi parquet (idempotent: overwrite folder or upsert by read+merge outside)
        # Ở môi trường production bạn có thể muốn write atomic bằng saveAsTable / checkpoint
        mapping_updated.write.mode("overwrite").parquet(mapping_table_path)

    return mapping_updated

# --- Example usage ---
if __name__ == "__main__":
    spark = SparkSession.builder.appName("custid-uuid-mapping").getOrCreate()

    source_hudi_path = "/path/to/hudi/customer_information"
    mapping_path = "/path/to/hudi/customer_uuid_map"  # Hudi or parquet folder
    updated_map_df = generate_or_update_uuid_mapping(
        spark,
        source_hudi_path=source_hudi_path,
        mapping_table_path=mapping_path,
        mapping_table_is_hudi=True,
        sample_fraction=0.5,
        mapping_hudi_table_name="customer_uuid_map"
    )

    print("Mapping now has", updated_map_df.count(), "rows.")
