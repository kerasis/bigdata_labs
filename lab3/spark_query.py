from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder \
    .appName("FlightsAvgDelay") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/user/root/data_airlines")

result = df.select(
        "OP_CARRIER",
        col("ARR_DELAY").cast("double").alias("ARR_DELAY")
    ) \
    .where(col("ARR_DELAY") > 0.0) \
    .groupBy("OP_CARRIER") \
    .agg(avg("ARR_DELAY").alias("avg_arr_delay"))

result.orderBy(result.avg_arr_delay.desc()).show()

spark.stop()
