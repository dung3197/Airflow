from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

jdbc_url = "jdbc:postgresql://postgres-host:5432/test_map"

jdbc_props = {
    "user": "postgres_user",
    "password": "postgres_password",
    "driver": "org.postgresql.Driver"
}

# Example source dataframe
# df contains column: c_customer_code

# -------------------------------------------------------------------
# 1. Read existing values from PostgreSQL
# IMPORTANT:
# PostgreSQL column is uppercase -> must use double quotes
# -------------------------------------------------------------------

postgres_df = (
    spark.read
        .jdbc(
            url=jdbc_url,
            table='''
            (
                SELECT "C_CUSTOMER_CODE"
                FROM public.t_back_account_map
            ) tmp
            ''',
            properties=jdbc_props
        )
)

# -------------------------------------------------------------------
# 2. Rename postgres column to match Spark dataframe column
# -------------------------------------------------------------------

postgres_df = postgres_df.withColumnRenamed(
    "C_CUSTOMER_CODE",
    "c_customer_code"
)

# -------------------------------------------------------------------
# 3. Rows that ALREADY EXIST in PostgreSQL
# -------------------------------------------------------------------

existing_df = (
    df.alias("src")
      .join(
          postgres_df.alias("pg"),
          on="c_customer_code",
          how="inner"
      )
)

# -------------------------------------------------------------------
# 4. Rows that DO NOT EXIST in PostgreSQL
# -------------------------------------------------------------------

not_existing_df = (
    df.alias("src")
      .join(
          postgres_df.alias("pg"),
          on="c_customer_code",
          how="left_anti"
      )
)

# -------------------------------------------------------------------
# 5. Debug
# -------------------------------------------------------------------

print("Existing rows:", existing_df.count())
print("Non-existing rows:", not_existing_df.count())

existing_df.show(truncate=False)
not_existing_df.show(truncate=False)
