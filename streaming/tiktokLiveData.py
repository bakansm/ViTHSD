import datetime as dt

from TikTokLive import TikTokLiveClient
from TikTokLive.types.events import CommentEvent, ConnectEvent
from kafkaHelper import initProducer, produceRecord
from predict import predict

client: TikTokLiveClient = TikTokLiveClient(unique_id="@chuonggachoi21")
producer = initProducer()

@client.on("connect")
async def on_connect(_: ConnectEvent):
    print("Connected to Room ID:", client.room_id)

async def on_comment(event: CommentEvent):
    predicted_data = predict(event.comment)

    data = {
        'timestamp': dt.datetime.now().timestamp(),
        'datetime': dt.datetime.now(),
        'userid': event.user.unique_id,
        'message': event.comment,
        'predict': predicted_data
    }

    produceRecord(predicted_data, producer=producer, topic='tiktok')
    print('Record: {}'.format(event.comment))

client.add_listener("comment", on_comment)

if __name__ == '__main__':
    client.run()
