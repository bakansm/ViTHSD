import json, requests, time, asyncio
import numpy as np
import datetime as dt
import numpy as np
import pandas as pd

from tensorflow.keras.models import load_model
from keras.preprocessing.text import Tokenizer
from TikTokLive import TikTokLiveClient
from TikTokLive.types.events import CommentEvent, ConnectEvent
from kafkaHelper import initProducer, produceRecord
from keras.utils import pad_sequences

# Instantiate the client with the user's username
client: TikTokLiveClient = TikTokLiveClient(unique_id="@chuonggachoi21")
producer = initProducer()
# Define how you want to handle specific events via decorator
@client.on("connect")
async def on_connect(_: ConnectEvent):
    print("Connected to Room ID:", client.room_id)

# Notice no decorator?
async def on_comment(event: CommentEvent):
    record = event.comment
    # df = pd.DataFrame({'Content': event.comment},index=[0])
    # tokenizer = Tokenizer()
    # tokenizer.fit_on_texts(df['Content'])
    # max_len = 100
    # inputData = tokenizer.texts_to_sequences(df['Content'].values.astype(str))
    # inputData = pad_sequences(inputData, maxlen=max_len, padding='post')
    # model = load_model('/home/bakansm/Code/FinalExam/model/model.h5')
    # pred = model.predict(inputData)
    # pred = np.array2string(pred)
    # record = {'Content': event.comment, "Predict": pred}
    produceRecord(record, producer=producer, topic='tiktok')
    print('Record: {}'.format(record))

# Define handling an event via "callback"
client.add_listener("comment", on_comment)

if __name__ == '__main__':
    # Run the client and block the main thread
    # await client.start() to run non-blocking
    client.run()
