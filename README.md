# ViTHSD-Vietnamese-Targeted-Hate-Speech-Detection
Vietnamese Targeted Hate Speech Detection on Social Media Texts.  
Contact information: Mr. Son T. Luu  
Email: sonlt@uit.edu.vn (Alternative: son.lt1103@gmail.com)

## Data
10,000 comments, each comment has 05 targets with three relevant hateful levels.   

## Use the dataset:
Please find the dataset directly from hugging-face here: https://huggingface.co/datasets/sonlam1102/vithsd.  

## Publication 
https://arxiv.org/abs/2404.19252    
(Please cite this paper when using the dataset)

Citation:   
Vo, C.N., Huynh, K.B., Luu, S.T. et al. ViTHSD: exploiting hatred by targets for hate speech detection on Vietnamese social media texts. J Comput Soc Sc 8, 30 (2025). https://doi.org/10.1007/s42001-024-00348-6

```
@article{vo2025vithsd,
  title={ViTHSD: exploiting hatred by targets for hate speech detection on Vietnamese social media texts},
  author={Vo, Cuong Nhat and Huynh, Khanh Bao and Luu, Son T and Do, Trong-Hop},
  journal={Journal of Computational Social Science},
  volume={8},
  number={2},
  pages={30},
  year={2025},
  publisher={Springer}
}
```

## Model

Updating

## Streaming

### Technologies

- Apache Kafka
- Apache Spark Structured Streaming
- QuestDB - for sink
  
### Requirements

- [Apache Kafka](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.4.0/kafka_2.13-3.4.0.tgz)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Questdb](https://questdb.io/)

### How to run

- **Step 1**: Start zookeeper server and kafka server
Code:
  - Start zookeeper server\
  `bin/zookeeper-server-start.sh config/zookeeper.properties`

  - Start kafka server\
  `bin/kafka-server-start.sh config/server.properties`

- **Step 2**: Create topic
  - Create topic named "youtube"\
  `bin/kafka-topics.sh --create --topic youtube --bootstrap-server localhost:9092`

- **Step 3**: Start questdb and connect questdb to topic.
  - Start questdb\
  `sudo questdb start`
  - Connect questdb connector to kafka topic\
  `bin/connect-standalone.sh config/connect-standalone.properties config/questdb-connector.properties`

- **Step 4**: Submit spark to kafka topic\
  `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 sparkStreaming.py`

- **Step 5**: Start producer and consumer
  - Producer\
    `python3 youtubeLiveData.py`

  - Consumer\
    `python3 consumer.py`

Now you can see the data on questdb at [here](localhost:9000)

## Application

Updating at [here](https://github.com/khanhvpro987/youtube-hsd)
