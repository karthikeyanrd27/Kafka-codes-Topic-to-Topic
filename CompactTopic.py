from pyspark import SparkConf, SparkContext
from operator import add
import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def handler(message):
    records = message.collect()
    for record in records:
        value_all=record[1]
        value_spt=value_all.split('|')
        value_key=value_spt[0]
        print (value_key)
        producer.send('COMPACTTOPIC', key=str(value_key),value=str(record[1]))
        producer.flush()

def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 10)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, ['TBL_MS_PLAN_WK'], {"metadata.broker.list": 'localhost:9092'})
    kvs.foreachRDD(handler)

    ssc.start()
    ssc.awaitTermination()
if __name__ == "__main__":

   main()
