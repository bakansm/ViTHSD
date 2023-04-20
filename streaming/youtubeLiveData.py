import pytchat

from predict import predict
from kafkaHelper import initProducer, produceRecord

producer = initProducer()
TOPIC = 'youtube'
VID_ID = "yQ3zrHaBAKA"

# Streaming Data
chat = pytchat.create(video_id=VID_ID)

if(chat.is_alive()):
    print("Livestream chat connected successfully")
    while chat.is_alive():
        for raw_data in chat.get().sync_items():
            
            predict_data = predict(raw_data.message)

            data = {
                'timestamp': raw_data.timestamp,
                'datetime': raw_data.datetime, 
                'userid': raw_data.author.channelId,
                'username': raw_data.author.name,
                'message': raw_data.message,
                'predict': predict_data
            }

            produceRecord(data, producer, TOPIC)
            print(data)
