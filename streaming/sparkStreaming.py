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

spark = SparkSession \
    .builder \
    .appName("Kafka streaming test") \
    .master('local[*]') \
    .getOrCreate()

schema = StructType([
  StructField("timestamp", TimestampType(), True),
  StructField("datetime" , DateType(), True),
  StructField("userid", StringType(), True),
  StructField("username", StringType(), True),
  StructField("message" , StringType(), True),
  StructField("predict", ArrayType(StringType()) , True),
])

# Read Dataframe
jsonData = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("subscribe", KAFKA_TOPIC) \
  .option("startingOffsets", "earliest") \
  .option("includeHeaders", "true") \
  .option("failOnDataLoss","false") \
  .load()

# Dataframe handle
df_json = jsonData \
          .select(from_json(col("value").cast("string"), schema).alias("value")) \
          .select(col("value.*"))

df_grouped = df_json \
          .withColumn("predict", explode(col("predict"))) \
          .groupBy("predict").count()

df_clean = df_grouped \
          .filter(col('predict').isin(["individual#clean", "groups#clean", "race#clean","religion#clean","politics#clean"])) \
          .withColumn('label', lit('clean')) \
          .groupBy(col('label')) \
          .sum('count')

# df_offensive = df_grouped \
#           .filter(col('predict').isin(["individual#offensive", "groups#offensive", "race#offensive","religion#offensive","politics#offensive"])) \
#           .withColumn('label', lit('offensive')) \
#           .groupBy(col('label')) \
#           .sum('count')

# df_hate = df_grouped \
#           .filter(col('predict').isin(["individual#hate", "groups#hate", "race#hate","religion#hate","politics#hate"])) \
#           .withColumn('label', lit('hate')) \
#           .groupBy(col('label')) \
#           .sum('count')

# df_individual = df_grouped \
#           .filter(col('predict').isin(["individual#clean", "individual#offensive", "individual#hate"])) \
#           .withColumn('target', lit('invididual')) \
#           .groupBy(col('target')) \
#           .sum('count')

# df_groups = df_grouped \
#           .filter(col('predict').isin(["groups#clean", "groups#offensive", "groups#hate"])) \
#           .withColumn('target', lit('groups')) \
#           .groupBy(col('target')) \
#           .sum('count')
 
# df_religion = df_grouped \
#           .filter(col('predict').isin(["religion#clean", "religion#offensive", "religion#hate"])) \
#           .withColumn('target', lit('religion')) \
#           .groupBy(col('target')) \
#           .sum('count')

# df_race = df_grouped\
#           .filter(col('predict').isin(["race#clean", "race#offensive", "race#hate"])) \
#           .withColumn('target', lit('race')) \
#           .groupBy(col('target')) \
#           .sum('count')

# df_politics = df_grouped\
#           .filter(col('predict').isin(["politics#clean", "politics#offensive", "politics#hate"]))\
#           .withColumn('target', lit('politics'))  \
#           .groupBy(col('target')) \
#           .sum('count')


# Write Datastream
ds = df_json \
  .select(to_json(struct("*")).alias("value")) \
  .writeStream \
  .format("kafka") \
  .trigger(processingTime="3 seconds") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("checkpointLocation", CHECKPOINT_LOCATION) \
  .outputMode("append") \
  .option("topic", "result") \
  .start()

ds_clean = df_clean \
  .select(to_json(struct("*")).alias("value")) \
  .writeStream \
  .format("kafka") \
  .trigger(processingTime="1 seconds") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("checkpointLocation",CHECKPOINT_LOCATION) \
  .outputMode("append") \
  .option("topic", "result") \
  .start()

# ds_offensive = df_offensive \
#   .select(to_json(struct("*")).alias("value")) \
#   .writeStream \
#   .format("kafka") \
#   .trigger(processingTime="3 seconds") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#   .option("checkpointLocation", CHECKPOINT_LOCATION) \
#   .outputMode("append") \
#   .option("topic", "result") \
#   .start()

# ds_hate = df_hate \
#   .select(to_json(struct("*")).alias("value")) \
#   .writeStream \
#   .format("kafka") \
#   .trigger(processingTime="3 seconds") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#   .option("checkpointLocation", CHECKPOINT_LOCATION) \
#   .outputMode("append") \
#   .option("topic", "result") \
#   .start()

# ds_individual = df_individual \
#   .select(to_json(struct("*")).alias("value")) \
#   .writeStream \
#   .format("kafka") \
#   .trigger(processingTime="3 seconds") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#   .option("checkpointLocation", CHECKPOINT_LOCATION) \
#   .outputMode("append") \
#   .option("topic", "result") \
#   .start()

# ds_groups = df_groups \
#   .select(to_json(struct("*")).alias("value")) \
#   .writeStream \
#   .format("kafka") \
#   .trigger(processingTime="3 seconds") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#   .option("checkpointLocation", CHECKPOINT_LOCATION) \
#   .outputMode("append") \
#   .option("topic", "result") \
#   .start()

# ds_religion = df_religion \
#   .select(to_json(struct("*")).alias("value")) \
#   .writeStream \
#   .format("kafka") \
#   .trigger(processingTime="3 seconds") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#   .option("checkpointLocation", CHECKPOINT_LOCATION) \
#   .outputMode("append") \
#   .option("topic", "result") \
#   .start()

# ds_race = df_race \
#   .select(to_json(struct("*")).alias("value")) \
#   .writeStream \
#   .format("kafka") \
#   .trigger(processingTime="3 seconds") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#   .option("checkpointLocation", CHECKPOINT_LOCATION) \
#   .outputMode("append") \
#   .option("topic", "result") \
#   .start()

# ds_politics = df_politics \
#   .select(to_json(struct("*")).alias("value")) \
#   .writeStream \
#   .format("kafka") \
#   .trigger(processingTime="3 seconds") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#   .option("checkpointLocation", CHECKPOINT_LOCATION) \
#   .outputMode("append") \
#   .option("topic", "result") \
#   .start()

ds.awaitTermination()