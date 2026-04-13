# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import DecimalType

# spark = SparkSession.builder \
#     .appName("Read Avro Example") \
#     .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
#     .getOrCreate()

# def cast_decimals_to_string(df):
#     exprs = []
#     for field in df.schema.fields:
#         if isinstance(field.dataType, DecimalType):
#             exprs.append(F.col(field.name).cast("string").alias(field.name))
#         else:
#             exprs.append(F.col(field.name))
#     return df.select(*exprs)
# df = spark.read.format("avro").load("/home/dyan/test-code/CUSTOMER_2026-04-12_10-43-42.070 (1).avro")
# df = cast_decimals_to_string(df)

# df.show()
# df.printSchema()


import json
from copy import deepcopy
from fastavro import reader
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

spark = SparkSession.builder \
    .appName("Read Avro Example") \
    .getOrCreate()

def load_avro_schema(avro_file_path: str) -> dict:
    with open(avro_file_path, "rb") as f:
        r = reader(f)
        # writer_schema is usually present in modern fastavro
        if hasattr(r, "writer_schema") and r.writer_schema is not None:
            return r.writer_schema
        # fallback for some versions
        if hasattr(r, "schema") and r.schema is not None:
            return r.schema
        raise ValueError("Cannot extract Avro schema from file")

def strip_oversized_decimals(schema_obj):
    """
    Recursively replace decimal logical types whose precision > 38
    with their underlying Avro physical type so Spark can read them.

    Avro decimal logical types annotate 'bytes' or 'fixed'. We remove
    logicalType/precision/scale and keep the underlying binary type. 
    """
    if isinstance(schema_obj, list):
        return [strip_oversized_decimals(x) for x in schema_obj]

    if isinstance(schema_obj, dict):
        obj = deepcopy(schema_obj)

        # Handle logical decimal
        if obj.get("logicalType") == "decimal":
            precision = obj.get("precision")
            if precision is not None and precision > 38:
                # Keep the physical Avro type, drop decimal annotations
                obj.pop("logicalType", None)
                obj.pop("precision", None)
                obj.pop("scale", None)
                return obj

        # Recurse into complex schemas
        t = obj.get("type")

        if t == "record":
            obj["fields"] = [
                {
                    **field,
                    "type": strip_oversized_decimals(field["type"])
                }
                for field in obj.get("fields", [])
            ]
            return obj

        if t == "array":
            obj["items"] = strip_oversized_decimals(obj["items"])
            return obj

        if t == "map":
            obj["values"] = strip_oversized_decimals(obj["values"])
            return obj

        if isinstance(t, (dict, list)):
            obj["type"] = strip_oversized_decimals(t)
            return obj

        return obj

    return schema_obj

# ---- usage ----
path = "/home/dyan/test-code/CUSTOMER_2026-04-12_10-43-42.070 (1).avro"

writer_schema = load_avro_schema(path)
safe_schema = strip_oversized_decimals(writer_schema)

df = (
    spark.read
         .format("avro")
         .option("avroSchema", json.dumps(safe_schema))
         .load(path)
)

df.show(truncate=False)
df.printSchema()







def handle_map_key_as_column(nested_df, map_cols):
    """
    Flattens DataFrame columns containing map-type elements by separating keys as individual columns.
    """
    original_cols = [col_name for col_name in nested_df.columns]
    for c in map_cols:
        keys_df = nested_df.select(explode(map_keys(col(c)))).distinct()
        keys = list(map(lambda row: row[0], keys_df.collect()))
        key_cols = list(map(lambda f: col(c).getItem(f).alias(str(f)), keys))
        nested_df = nested_df.select([col_name for col_name in original_cols if col_name != c] + key_cols)
    nested_df = nested_df.select([col_name for col_name in nested_df.columns if col_name not in map_cols])
    return nested_df



def handle_map(nested_df, map_cols):
    """
    Flattens DataFrame columns containing map-type elements by exploding map elements into separate rows.
    """
    original_cols = [col_name for col_name in nested_df.columns]
    for c in map_cols:
        not_map_cols = [col_name for col_name in nested_df.columns if col_name not in map_cols]
        nested_df = nested_df.select(*not_map_cols, explode(c).alias(f"{c}_key", f"{c}_value"))
    nested_df = nested_df.select([col_name for col_name in nested_df.columns if col_name not in map_cols])
    return nested_df


    
def handle_struct(df, struct_cols):
    """
    Flattens DataFrame columns containing struct-type elements by expanding nested fields as individual columns.
    """
    flat_cols = [col(c) for c in df.columns if c not in struct_cols]
    struct_col_expressions = []
    for i in struct_cols:
        projected_df = df.select(i + ".*")
        for c in projected_df.columns:
            struct_col_expressions.append(col(f"{i}.{c}").alias(f"{i}_{c}"))
    return df.select(flat_cols + struct_col_expressions)

def handle_array(df, array_cols):
    """
    Flattens DataFrame columns containing array-type elements by exploding the array into separate rows.
    """
    for c in array_cols:
        df = df.withColumn(c, explode(c))
    return df

def flatten_df(df):
    """
    Dynamically flattens nested DataFrame structures.
    """
    struct_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]
    array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
    map_cols = [c[0] for c in df.dtypes if c[1][:3] == "map"]

    while len(array_cols) > 0 or len(struct_cols) > 0 or len(map_cols) > 0:
        if array_cols:
            df = handle_array(df, array_cols)
        if struct_cols:
            df = handle_struct(df, struct_cols)
        if map_cols:
            df = handle_map_key_as_column(df, map_cols)
            #df = handle_map(df, map_cols)

        array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        struct_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]
        map_cols = [c[0] for c in df.dtypes if c[1][:3] == "map"]

    return df
