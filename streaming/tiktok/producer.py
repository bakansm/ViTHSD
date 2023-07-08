import datetime as dt
import time 
import sys

from TikTokLive import TikTokLiveClient
from TikTokLive.types.events import CommentEvent, ConnectEvent
from kafkaHelper import initProducer, produceRecord
from predict import predict

VIDEO_ID = sys.argv[1]
client: TikTokLiveClient = TikTokLiveClient(unique_id=VIDEO_ID)
producer = initProducer()

@client.on("connect")
async def on_connect(_: ConnectEvent):
    print("Connected to Room ID:", client.room_id)

async def on_comment(event: CommentEvent):
    start = time.time()
    predicted_data = predict(event.comment)
    end = time.time()
    predict_time = end - start

    data = {
        'timestamp': dt.datetime.now().timestamp(),
        'datetime': dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'userid': event.user.user_id,
        'username': event.user.nickname,
        'message': event.comment,
        'predict': predicted_data,
        'predict-time': predict_time,
    }
    produceRecord(data, producer=producer, topic=VIDEO_ID)

client.add_listener("comment", on_comment)

if __name__ == '__main__':
    client.run()
