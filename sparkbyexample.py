from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, substring

spark=SparkSession.builder.appName("stringoperations").getOrCreate()

# Create Sample Data
data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df=spark.createDataFrame(data,columns)

# Using substring()
df.withColumn('year', substring('date', 1,4))\
    .withColumn('month', substring('date', 5,2))\
    .withColumn('day', substring('date', 7,2))
df.printSchema()
df.show(truncate=False)

# substring() with select()
df.select('date', substring('date', 1,4).alias('year'), \
                  substring('date', 5,2).alias('month'), \
                  substring('date', 7,2).alias('day'))  
