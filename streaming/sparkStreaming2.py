from pyspark.sql import Window
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession 
from config import config 
# from pyspark.ml.feature import StringIndexer 
# from pyspark.ml.feature import VectorAssembler


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
CHECKPOINT_LOCATION = "./checkpoint"
KAFKA_TOPIC = 'youtube'

spark = SparkSession.builder \
    .appName("Kafka streaming test") \
    .master('local[*]') \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("datetime", DateType(), True),
    StructField("userid", StringType(), True),
    StructField("username", StringType(), True),
    StructField("message", StringType(), True),
    StructField("predict", ArrayType(StringType()), True),
])

# Read Dataframe
jsonData = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .option("failOnDataLoss", "false") \
    .load()

# Dataframe handle
df_json = jsonData \
    .select(from_json(col("value").cast("string"), schema).alias("value")) \
    .select(col("value.*"))

# Aggregate queries
df_grouped = df_json \
    .select(explode(col("predict")).alias("predict"))

# Query 1: Count
df_clean_count = df_grouped \
    .groupBy("predict") \
    .count() \
    .filter(col('predict').isin(["individual#clean", "groups#clean", "race#clean","religion#clean","politics#clean"])) \
    .withColumn('label', lit('clean')) \
    .groupBy(col('label')) \
    .sum('count')

# Query 2: Sum
df_clean_sum = df_grouped \
    .filter(col('predict').isin(["individual#clean", "groups#clean", "race#clean","religion#clean","politics#clean"])) \
    .groupBy(col('predict')) \
    .agg(sum(lit(1)).alias("total"))

# Query 3: Average
df_clean_avg = df_grouped \
    .filter(col('predict').isin(["individual#clean", "groups#clean", "race#clean","religion#clean","politics#clean"])) \
    .groupBy(col('predict')) \
    .agg(avg(lit(1)).alias("average"))

# Write Datastream
query_count = df_clean_count \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .trigger(processingTime="3 seconds") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("checkpointLocation", "./checkpoint/count") \
    .outputMode("update") \
    .option("topic", "result_count") \
    .start()

query_sum = df_clean_sum \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .trigger(processingTime="3 seconds") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("checkpointLocation", "./checkpoint/sum") \
    .outputMode("update") \
    .option("topic", "result_sum") \
    .start()

query_avg = df_clean_avg \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .trigger(processingTime="3 seconds") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("checkpointLocation", "./checkpoint/avg") \
    .outputMode("update") \
    .option("topic", "result_avg") \
    .start()

query_count.awaitTermination()
query_sum.awaitTermination()
query_avg.awaitTermination()

