import pandas as pd 
import numpy as np 
import datetime as dt
# from pymongo.mongo_client import MongoClient
# from pymongo.server_api import ServerApi

from kafkaHelper import consumeRecord, initConsumer

print('Starting Apache Kafka consumers')
consumer = initConsumer('youtube')

# uri = "mongodb+srv://bakansm:Khanhcool2001@kafkasink.6c3trd5.mongodb.net/?retryWrites=true&w=majority"
# client = MongoClient(uri, server_api=ServerApi('1'))
# db = client['YoutubeHSD']
# collection = db['YoutubeHSD']

while True:
    records = consumeRecord(consumer)
    for r in records:
        print(r)
        # collection.insert_one(r).inserted_id