import threading
import time
import subprocess

from predict import predict
from flask import Flask, request
from flask_cors import cross_origin, CORS
from functools import wraps

# Create a Flask app
app = Flask(__name__)
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

def start_zookeeper ():
    # Start ZooKeeper server
    zookeeper_cmd = "~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties &"
    subprocess.Popen(zookeeper_cmd, shell=True)
    time.sleep(6)
    subprocess.run("clear", shell=True)

def start_kafka ():
# Start Kafka server
    kafka_cmd = "~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties &"
    subprocess.Popen(kafka_cmd, shell=True)
    time.sleep(6)
    subprocess.run("clear", shell=True)

def unpack(response, default_code=200):
    if not isinstance(response, tuple):
        # data only
        return response, default_code, {}
    elif len(response) == 1:
        # data only as tuple
        return response[0], default_code, {}
    elif len(response) == 2:
        # data and code
        data, code = response
        return data, code, {}
    elif len(response) == 3:
        # data, code and headers
        data, code, headers = response
        return data, code or default_code, headers
    else:
        raise ValueError("Too many response values")

def enable_cors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        data, code, headers = unpack(func(*args, **kwargs))

        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = '*'
        headers['Access-Control-Allow-Headers'] = '*'
        headers['Access-Control-Max-Age'] = '1728000'
        headers['Access-Control-Expose-Headers'] = 'ETag, X-TOKEN'

        return data, code, headers

    return wrapper

@app.route('/predict', methods=['POST'])
@cross_origin()
def text_predict():
    # Get the form data from the request
    payload = request.get_json()
    message = payload["message"]
    label = predict(message)
    print(label)
    # Return a response to the React form
    return {"code": 200, "data": {"label": label}, "msg": "Success"}

def youtube_consumer (id):
    kafka_cmd = f"python3 ~/Code/ViTHSD-Vietnamese-Targeted-Hate-Speech-Detection/streaming/youtube/consumer.py {id} &"
    subprocess.Popen(kafka_cmd, shell=True)

def youtube_producer (id):
    kafka_cmd = f"python3 ~/Code/ViTHSD-Vietnamese-Targeted-Hate-Speech-Detection/streaming/youtube/producer.py {id} &"
    subprocess.Popen(kafka_cmd, shell=True)

@app.route('/streaming/youtube', methods=['POST'])
@cross_origin()
def stream_youtube():
    payload = request.get_json()
    message = payload["message"]
    consumer_thread = threading.Thread(target=youtube_consumer, args=(message,))
    producer_thread = threading.Thread(target=youtube_producer, args=(message,))
    
    consumer_thread.start()
    producer_thread.start()

    return {"code": 200, "data": {"label": message}, "msg": "Success"}

def tiktok_consumer (id):
    kafka_cmd = f"python3 ~/Code/ViTHSD-Vietnamese-Targeted-Hate-Speech-Detection/streaming/tiktok/consumer.py {id} &"
    subprocess.Popen(kafka_cmd, shell=True)

def tiktok_producer (id):
    kafka_cmd = f"python3 ~/Code/ViTHSD-Vietnamese-Targeted-Hate-Speech-Detection/streaming/tiktok/producer.py {id} &"
    subprocess.Popen(kafka_cmd, shell=True)

@app.route('/streaming/tiktok', methods=['POST'])
@cross_origin()
def stream_tiktok():
    payload = request.get_json()
    message = payload["message"]
    consumer_thread = threading.Thread(target=tiktok_consumer, args=(message,))
    producer_thread = threading.Thread(target=tiktok_producer, args=(message,))
    
    consumer_thread.start()
    producer_thread.start()

    return {"code": 200, "data": {"label": message}, "msg": "Success"}

if __name__ == '__main__':
    start_zookeeper()
    time.sleep(10)
    start_kafka()
    time.sleep(10)
    app.run(host='0.0.0.0', debug=True)
