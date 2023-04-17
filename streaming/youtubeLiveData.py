import pytchat
import pickle
import tensorflow as tf
import numpy as np
import pandas as pd

from preprocessing import preprocessing
from keras.utils import pad_sequences
from transformers import AutoTokenizer, TFAutoModel, TFXLMRobertaModel
from kafkaHelper import initProducer, produceRecord

producer = initProducer()
TOPIC = 'youtube'
VID_ID = "6yKh7DqPSy8"

# Model
class TargetedHSD:
    def __init__(self, model_path = None, tokenizer_path = None):
        if not model_path:
            self.__model_path = '../saved_model/bigrulstmcnn_xlmr2.h5'
        else:
            self.__model_path = model_path
        if not tokenizer_path:
            self.__tokenizer_path = 'xlm-roberta-base'
        else:
            self.__tokenizer_path = tokenizer_path
        

        self._tokenizer = AutoTokenizer.from_pretrained(self.__tokenizer_path)
        self._model = tf.keras.models.load_model(self.__model_path, custom_objects={'TFXLMRobertaModel': TFXLMRobertaModel})
        self.result = None
        self.orginal_label = None
    
    def predict(self, text):
        # encoded_text = self._tokenizer.texts_to_sequences([text])
        # encoded_text = pad_sequences(encoded_text, maxlen=100, padding='post')
        encoded_text = np.array(self._tokenizer([text], max_length=100, padding='max_length', truncation=True)['input_ids'])
        encoded_text = {
            "input_ids": np.asarray(self._tokenizer([text], max_length=50, padding='max_length', truncation=True)['input_ids']),
            "attention_mask": np.asarray(self._tokenizer([text], max_length=50, padding='max_length', truncation=True)['attention_mask'])
        }
        pred = self._model.predict(encoded_text)
        pred = np.argmax(pred.reshape(-1, 5, 4), axis=-1)

        # y_test_pred_new = []
        # for y in pred:
        #     lb = []
        #     for i in range(0, len(y)):
        #         if y[i] >= 0.5:
        #             lb.append(1)
        #         else:
        #             lb.append(0)
        #     y_test_pred_new.append(lb)
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
        # LABEL = [('individual', 1),
        #             ('individual', 2),
        #             ('individual', 3),
        #             ('groups', 1),
        #             ('groups', 2),
        #             ('groups', 3),
        #             ('religion/creed', 1),
        #             ('religion/creed', 2),
        #             ('religion/creed', 3),
        #             ('race/ethnicity', 1),
        #             ('race/ethnicity', 2),
        #             ('race/ethnicity', 3),
        #             ('politics', 1),
        #             ('politics', 2),
        #             ('politics', 3)
        #         ]
        print(self.orginal_label)
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
            preprocessed_data = preprocessing(raw_data.message)
            cls.predict(preprocessed_data)

            data = {
                'timestamp': raw_data.timestamp,
                'datetime': raw_data.datetime, 
                'userid': raw_data.author.channelId,
                'username': raw_data.author.name,
                'message': preprocessed_data,
                'predict': cls.return_label()
            }

            produceRecord(data, producer, TOPIC)
            print(data)
