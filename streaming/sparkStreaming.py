from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from config import config, params
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
# KAFKA_TOPIC = "{0},{1},{2},{3},{4},{5},{6}" \
#               .format(config['topic_1'],
#                       config['topic_2'],
#                       config['topic_3'],
#                       config['topic_4'],
#                       config['topic_5'],
#                       config['topic_6'],
#                       config['topic_7'])
CHECKPOINT_LOCATION = "./checkpoint"

KAFKA_TOPIC = 'youtube'


### Khởi tạo spark
spark = SparkSession \
    .builder \
    .appName("Kafka streaming test") \
    .master('local[*]') \
    .getOrCreate()


### Tạo khung dataframe
schema = StructType([ 
  StructField("timestamp", TimestampType(), True),
  StructField("datetime" , DateType(), True),
  StructField("userid", StringType(), True),
  StructField("username", StringType(), True),
  StructField("message" , StringType(), True),
  StructField("predict", StringType() , True),
])


### Lấy dữ liệu từ kafka ( định dạng json )
jsonData = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("subscribe", KAFKA_TOPIC) \
  .option("startingOffsets", "earliest") \
  .option("includeHeaders", "true") \
  .option("failOnDataLoss","false") \
  .load()

### Chuyển json sang datafram pyspark
dataframe = jsonData.select(from_json(col("value").cast("string"), schema).alias("value")).select(col("value.*"))

# dataframe = dataframe.groupBy('currency').agg(avg('amount').cast('Float').alias('average'), current_timestamp().alias('date'))

# dataframe.printSchema()

# ############################# IMPORTANT #############################
# ### ---> Code mô hình máy học ở đây khúc này bằng dataframe  <--- ###
# #####################################################################

### Chuyển dataframe ngược lại json để thêm vào topic
reward_data = dataframe.select(to_json(struct("*")).alias("value"))

### Thêm data vào lại topic
ds = reward_data \
  .writeStream \
  .format("kafka") \
  .trigger(processingTime="1 seconds") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("checkpointLocation", CHECKPOINT_LOCATION) \
  .outputMode("update") \
  .option("topic", "result") \
  .start()

ds.awaitTermination()