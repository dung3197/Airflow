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