from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import PurePosixPath

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.column import Column


# ============================================================
# 1) Config defaults
# ============================================================

DEFAULT_FILE_COUNT = 100
DEFAULT_TARGET_FILE_SIZE_MB = 128

# IMPORTANT:
# Spark controls output file size primarily by record count, not bytes.
# Start with this value, then tune after first run if needed.
#
# If your generated files are too small:
#   increase RECORDS_PER_FILE_ESTIMATE
# If your generated files are too large:
#   decrease RECORDS_PER_FILE_ESTIMATE
#
# A practical starting point for this payload shape:
DEFAULT_RECORDS_PER_FILE_ESTIMATE = 90000

DEFAULT_OUTPUT_PATH = "hdfs:///tmp/ogg_customer_avro_128mb_batch"
TABLE_NAME = "STOCK.CUSTOMER"


# ============================================================
# 2) Column builders
# ============================================================

def customer_struct(customer_id_col: Column, full_name_col: Column) -> Column:
    """
    Build the nested customer struct for 'before' or 'after'.
    All business-date-like fields remain strings.
    last_trade_date is strict YYYY-MM-DD for Hudi partitioning.
    """
    customer_id_str = customer_id_col.cast("string")

    # Strict YYYY-MM-DD date string, cycling over a date range
    last_trade_date = F.date_format(
        F.date_add(F.to_date(F.lit("2024-10-01")), (customer_id_col % F.lit(120)).cast("int")),
        "yyyy-MM-dd",
    )

    base_text = F.concat(
        F.lit("Customer profile for "),
        full_name_col,
        F.lit(". Stock company customer record. customer_id="),
        customer_id_str,
        F.lit(". "),
    )

    custom_attrs = [
        F.concat(
            F.lit(f"custom-value-"),
            customer_id_str,
            F.lit(f"-{i} | Customer profile payload attribute-sequence={i}")
        ).alias(f"custom_attr_{i:03d}")
        for i in range(1, 41)
    ]

    return F.struct(
        customer_id_col.cast("long").alias("customer_id"),
        F.format_string("CUST%07d", customer_id_col).alias("customer_code"),
        F.lit("INDIVIDUAL").alias("customer_type"),
        full_name_col.alias("full_name"),
        full_name_col.alias("short_name"),
        F.when((customer_id_col % 2) == 0, F.lit("M")).otherwise(F.lit("F")).alias("gender"),
        F.lit("1990-05-20").alias("date_of_birth"),
        F.lit("VN").alias("nationality"),
        F.lit("SINGLE").alias("marital_status"),
        F.concat(F.lit("TAX"), F.lpad(customer_id_str, 8, "0")).alias("tax_code"),
        F.lit("ACTIVE").alias("customer_status"),
        F.when((customer_id_col % 5) == 0, F.lit("HIGH")).otherwise(F.lit("MEDIUM")).alias("risk_level"),
        F.lit("RETAIL").alias("segment"),
        F.format_string("BR%03d", customer_id_col % 20).alias("branch_code"),
        F.format_string("BRK%03d", customer_id_col % 100).alias("broker_code"),
        F.format_string("ACC%010d", customer_id_col).alias("account_no"),
        F.format_string("TRADE%010d", customer_id_col).alias("trading_account_no"),
        F.format_string("CSD%010d", customer_id_col).alias("custody_account_no"),
        F.concat(F.lit("customer"), customer_id_str, F.lit("@example.com")).alias("email"),
        F.substring(F.concat(F.lit("09"), F.lpad(customer_id_str, 8, "0")), 1, 10).alias("mobile_phone"),
        F.lit(None).cast("string").alias("home_phone"),
        F.lit(None).cast("string").alias("office_phone"),
        F.lit(None).cast("string").alias("fax"),
        F.concat(customer_id_str, F.lit(" Nguyen Hue Boulevard")).alias("address_line1"),
        F.lit("Tower A, Floor 12").alias("address_line2"),
        F.lit("Ben Nghe").alias("ward"),
        F.lit("District 1").alias("district"),
        F.lit("Ho Chi Minh City").alias("province"),
        F.lit("Vietnam").alias("country"),
        F.lit("700000").alias("postal_code"),
        F.concat(F.lit("Contact "), customer_id_str).alias("contact_person"),
        F.lit("Engineer").alias("occupation"),
        F.lit("Example Securities Client Company").alias("company_name"),
        F.lit("Senior Specialist").alias("job_title"),
        F.lit("500000000").alias("annual_income"),
        F.lit("1500000000").alias("net_worth"),
        F.lit("SALARY").alias("source_of_funds"),
        F.lit("5_YEARS").alias("investment_experience"),
        F.lit("MEDIUM").alias("risk_appetite"),
        F.lit("COMPLETED").alias("kyc_status"),
        F.lit("2025-01-10").alias("kyc_date"),
        F.lit(False).alias("aml_flag"),
        F.lit(False).alias("pep_flag"),
        F.lit(False).alias("sanction_flag"),
        F.lit(False).alias("fatca_flag"),
        F.lit(None).cast("string").alias("fatca_country"),
        F.lit(False).alias("crs_flag"),
        F.lit(None).cast("string").alias("crs_country"),
        F.lit("CCCD").alias("id_type"),
        F.substring(F.concat(F.lit("0790"), F.lpad(customer_id_str, 8, "0")), 1, 12).alias("id_number"),
        F.lit("2022-07-15").alias("id_issue_date"),
        F.lit("Cuc Canh Sat QLHC").alias("id_issue_place"),
        F.lit("2037-07-15").alias("id_expiry_date"),
        F.lit(None).cast("string").alias("passport_number"),
        F.lit(None).cast("string").alias("passport_issue_date"),
        F.lit(None).cast("string").alias("passport_expiry_date"),
        F.substring(F.concat(F.lit("9704"), F.lpad(customer_id_str, 12, "0")), 1, 16).alias("bank_account_no"),
        F.lit("VCB").alias("bank_name"),
        F.lit("Sai Gon").alias("bank_branch"),
        F.lit("BANK_TRANSFER").alias("settlement_method"),
        F.lit("25000000.50").alias("cash_balance"),
        F.lit("5000000.00").alias("margin_balance"),
        F.lit("0").alias("loan_balance"),
        F.lit("100000000").alias("credit_limit"),
        F.lit("95000000").alias("available_limit"),
        F.lit("230000000.75").alias("portfolio_value"),
        F.lit("1200000.25").alias("realized_pnl"),
        F.lit("3500000.10").alias("unrealized_pnl"),
        last_trade_date.alias("last_trade_date"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("last_login_ts"),
        F.lit("vi").alias("preferred_language"),
        F.lit("MOBILE_APP").alias("preferred_channel"),
        F.lit(True).alias("sms_opt_in"),
        F.lit(True).alias("email_opt_in"),
        F.lit(True).alias("mobile_app_opt_in"),
        F.lit(False).alias("marketing_opt_in"),
        ((customer_id_col % 10) == 0).alias("vip_flag"),
        ((customer_id_col % 7) == 0).alias("priority_flag"),
        F.lit(False).alias("blacklist_flag"),
        F.lit(False).alias("fraud_flag"),
        F.format_string("RM%03d", customer_id_col % 100).alias("relationship_manager"),
        F.format_string("REF%03d", customer_id_col % 1000).alias("referral_code"),
        F.concat(F.lit("Referrer "), customer_id_str).alias("referrer_name"),
        F.lit("2024-06-01").alias("opened_date"),
        F.lit(None).cast("string").alias("closed_date"),
        F.lit("SYSTEM").alias("created_by"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("created_ts"),
        F.lit("SYSTEM").alias("updated_by"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("updated_ts"),
        F.lit("SUPERVISOR01").alias("approved_by"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("approved_ts"),
        F.concat(base_text, base_text, base_text).alias("remark"),
        F.lit("CORE_CRM").alias("source_system"),
        F.concat(F.lit("EXT"), F.lpad(customer_id_str, 8, "0")).alias("external_customer_id"),
        F.lit("Y").alias("trading_permission"),
        F.lit("N").alias("derivative_permission"),
        F.lit("Y").alias("bond_permission"),
        F.lit("Y").alias("mutual_fund_permission"),
        F.lit(True).alias("online_trading_flag"),
        F.lit(True).alias("margin_trading_flag"),
        F.lit(True).alias("securities_depository_flag"),
        *custom_attrs
    )


# ============================================================
# 3) Hadoop FS helpers
# ============================================================

def list_avro_files_with_sizes(spark: SparkSession, output_path: str):
    """
    List .avro files and sizes using Hadoop FS API through the JVM.
    Works for HDFS-compatible paths visible to the driver.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path_obj = jvm.org.apache.hadoop.fs.Path(output_path)

    results = []
    for status in fs.listStatus(path_obj):
        p = status.getPath().toString()
        name = status.getPath().getName()
        if status.isFile() and name.endswith(".avro"):
            results.append((p, name, int(status.getLen())))
    return results


# ============================================================
# 4) Main job
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--file-count", type=int, default=DEFAULT_FILE_COUNT)
    parser.add_argument("--target-file-size-mb", type=int, default=DEFAULT_TARGET_FILE_SIZE_MB)
    parser.add_argument("--records-per-file", type=int, default=DEFAULT_RECORDS_PER_FILE_ESTIMATE)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append", "error", "ignore"])
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OGG Customer Avro Generator")
        # maxRecordsPerFile is a Spark SQL config for file writers
        .config("spark.sql.files.maxRecordsPerFile", args.records_per_file)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    total_records = args.file_count * args.records_per_file

    # Generate a deterministic unique id space: 1..N
    base = spark.range(1, total_records + 1).withColumnRenamed("id", "customer_id")

    op_mod = F.col("customer_id") % 3
    op_type = (
        F.when(op_mod == 1, F.lit("I"))
         .when(op_mod == 2, F.lit("U"))
         .otherwise(F.lit("D"))
    )

    full_name_insert = F.concat(F.lit("Customer "), F.col("customer_id").cast("string"))
    full_name_old = F.concat(F.lit("Customer "), F.col("customer_id").cast("string"), F.lit(" Old"))
    full_name_new = F.concat(F.lit("Customer "), F.col("customer_id").cast("string"), F.lit(" New"))
    full_name_delete = F.concat(F.lit("Customer "), F.col("customer_id").cast("string"))

    before_struct = (
        F.when(op_type == "I", F.lit(None))
         .when(op_type == "U", customer_struct(F.col("customer_id"), full_name_old))
         .otherwise(customer_struct(F.col("customer_id"), full_name_delete))
    )

    after_struct = (
        F.when(op_type == "D", F.lit(None))
         .when(op_type == "I", customer_struct(F.col("customer_id"), full_name_insert))
         .otherwise(customer_struct(F.col("customer_id"), full_name_new))
    )

    df = (
        base
        .select(
            op_type.alias("op_type"),
            F.lit(TABLE_NAME).alias("table"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("current_ts"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("op_ts"),
            before_struct.alias("before"),
            after_struct.alias("after"),
            (F.col("customer_id") + F.lit(100000000)).cast("string").alias("csn"),
            (F.col("customer_id") + F.lit(20000000000000000000)).cast("string").alias("pos"),
        )
        .repartition(args.file_count)
    )

    # Write Avro
    (
        df.write
        .format("avro")
        .mode(args.mode)
        .save(args.output_path)
    )

    # Build manifest log after write
    files = list_avro_files_with_sizes(spark, args.output_path)
    manifest_rows = []
    run_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    for path_str, file_name, size_bytes in files:
        manifest_rows.append((
            run_ts,
            path_str,
            file_name,
            size_bytes,
            round(size_bytes / (1024 * 1024), 2),
            args.records_per_file,
            args.target_file_size_mb,
        ))

    manifest_df = spark.createDataFrame(
        manifest_rows,
        schema="""
            run_ts string,
            file_path string,
            file_name string,
            size_bytes long,
            size_mb double,
            configured_records_per_file long,
            target_file_size_mb long
        """
    )

    manifest_output = str(PurePosixPath(args.output_path) / "_manifest")
    (
        manifest_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(manifest_output)
    )

    summary = manifest_df.agg(
        F.count("*").alias("actual_file_count"),
        F.sum("size_bytes").alias("total_size_bytes"),
        F.round(F.sum("size_mb"), 2).alias("total_size_mb"),
        F.round(F.avg("size_mb"), 2).alias("avg_file_size_mb"),
        F.min("size_mb").alias("min_file_size_mb"),
        F.max("size_mb").alias("max_file_size_mb"),
    ).collect()[0]

    print("Generation completed.")
    print(f"Output path            : {args.output_path}")
    print(f"Requested file count   : {args.file_count}")
    print(f"Actual Avro file count : {summary['actual_file_count']}")
    print(f"Configured rec/file    : {args.records_per_file}")
    print(f"Total size (MB)        : {summary['total_size_mb']}")
    print(f"Average file size (MB) : {summary['avg_file_size_mb']}")
    print(f"Min file size (MB)     : {summary['min_file_size_mb']}")
    print(f"Max file size (MB)     : {summary['max_file_size_mb']}")
    print(f"Manifest path          : {manifest_output}")

    spark.stop()


if __name__ == "__main__":
    main()
