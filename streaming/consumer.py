import pandas as pd 
import numpy as np 
import datetime as dt

from kafkaHelper import produceRecord, consumeRecord, initConsumer, initProducer
from config import config, params

# initialize Kafka consumers and producer
print('Starting Apache Kafka consumers and producer')
consumer = initConsumer('youtube')

# intialize local dataframe
data = pd.DataFrame(columns=['timestamp','datetime', 'id','username', 'message' ])

while True:
    records = consumeRecord(consumer)
    for r in records:
        print(r)
