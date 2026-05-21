from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

jdbc_url = "jdbc:postgresql://postgres-host:5432/mydb"

jdbc_props = {
    "user": "postgres_user",
    "password": "postgres_password",
    "driver": "org.postgresql.Driver"
}

target_table = "public.customer"

# Your source DataFrame
# df columns example: customer_id, name, age, created_at

# 1. Read only existing keys from PostgreSQL
existing_keys_df = (
    spark.read
        .jdbc(
            url=jdbc_url,
            table="(SELECT customer_id FROM public.customer) t",
            properties=jdbc_props
        )
)

# 2. Keep only rows whose customer_id does NOT exist in PostgreSQL
new_rows_df = (
    df.alias("src")
      .join(
          existing_keys_df.alias("pg"),
          on="customer_id",
          how="left_anti"
      )
)

# 3. Append missing rows to PostgreSQL
(
    new_rows_df.write
        .mode("append")
        .jdbc(
            url=jdbc_url,
            table=target_table,
            properties=jdbc_props
        )
)
