import pandas as pd 
import numpy as np 
import datetime as dt
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from kafkaHelper import consumeRecord, initConsumer

def kafka_consumer (id):
    print('Starting Apache Kafka consumers')
    consumer = initConsumer(id)
    
    uri = "mongodb+srv://bakansm:Khanhcool2001@kafkasink.6c3trd5.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client['ViHSD']
    collection = db[id]
    
    while True:
        records = consumeRecord(consumer)
        for r in records:
            collection.insert_one(r).inserted_id
        # consumer.close()
    