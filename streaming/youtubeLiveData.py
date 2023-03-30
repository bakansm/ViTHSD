import pytchat
import pickle
import tensorflow as tf
import numpy as np
import pandas as pd

from keras.utils import pad_sequences
from transformers import AutoTokenizer
from kafkaHelper import initProducer, produceRecord
from config import config, params

producer = initProducer()
TOPIC = 'youtube'
VID_ID = "RS8tg7Q1-hQ"


# Model
class TargetedHSD:
    def __init__(self, model_path = None, tokenizer_path = None):
        if not model_path:
            self.__model_path = '../model/xlm-roberta-base.h5'
        else:
            self.__model_path = model_path
        if not tokenizer_path:
            self.__tokenizer_path = 'xlm-roberta-base'
        else:
            self.__tokenizer_path = tokenizer_path
        
        self._tokenizer = AutoTokenizer.from_pretrained(self.__tokenizer_path)

        self.result = None
        self.orginal_label = None
    
    def predict(self, text):
        encoded_text = np.array(self._tokenizer([text], max_length=100, padding='max_length', truncation=True)['input_ids'])
        pred = np.argmax(encoded_text.reshape(-1, 5, 4), axis=-1)
        self.orginal_label = pred[0]
    
    def return_label(self):
        true_labels = []
        TYPE = {
            1: "clean",
            2: "offensive",
            3: "hate"
        }
        LABEL = {
            0: "individual",
            1: "groups",
            2: "religion",
            3: "race",
            4: "politics"
        }
        for i in range(0, len(self.orginal_label)):
            if self.orginal_label[i] > 0:
                t = LABEL[i] + "#" + TYPE[int(self.orginal_label[i])]
                true_labels.append(t)

        self.result = true_labels
        return true_labels 
    
# Load model
cls = TargetedHSD()

# Streaming Data
chat = pytchat.create(video_id=VID_ID)

if(chat.is_alive()):
    print("Livestream chat connected successfully")
    while chat.is_alive():
        for raw_data in chat.get().sync_items():

            # Predict
            cls.predict(raw_data.message)

            data = {
                'timestamp': raw_data.timestamp / 1000,
                'datetime': raw_data.datetime, 
                'userid': raw_data.author.channelId,
                'username': raw_data.author.name,
                'message': raw_data.message,
                'predict': cls.return_label()
            }

            produceRecord(data, producer, TOPIC)
            print(data)
