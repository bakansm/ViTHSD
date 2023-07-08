import pandas as pd 
import numpy as np 
import datetime as dt
import sys

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from kafkaHelper import consumeRecord, initConsumer

TOPIC = sys.argv[1]
print('Starting Apache Kafka consumers')
consumer = initConsumer(TOPIC)

uri = "mongodb+srv://bakansm:Khanhcool2001@kafkasink.6c3trd5.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['ViHSD']
collection = db[TOPIC]

while True:
    records = consumeRecord(consumer)
    for r in records:
        print(r)
        collection.insert_one(r).inserted_id
    