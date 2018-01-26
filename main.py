#!/usr/bin/env python
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import time
import os

parser = argparse.ArgumentParser()
parser.add_argument("-a",
                    help = "Kafka daemon to connect", default = '127.0.0.1')
parser.add_argument("-p",
                    help = "Port number of kafak daemon", default = '9092')
parser.add_argument("-w",
                    help = "Batch infuse interval", default = '4')
parser.add_argument("-s",
                    help = "Batch size", default = '1000')
parser.add_argument("-d",
                    help = "Data source directory", default = '')
parser.add_argument("-t",
                    help = "Kafka topic to send", default = 'test')

if __name__ == '__main__':
    # Setting up read in args
    args = parser.parse_args()
    directory = args.d
    port = args.p
    window = int(args.w)
    batch_size = int(args.s)
    addr = args.a
    t = args.t

    articles = []
    with open(os.path.join(directory, "articles.txt")) as f:
        articles.append([line.strip() for line in f.readlines() if line != '\n'])
    # Now article is read in Memory
    print(articles)

    producer = KafkaProducer(bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
                                 kafka_host = addr,
                                 kafka_port = port
                             ),value_serializer = str.encode)
    send_count = 0
    while True:
        for article in articles:
            for line in article:
                producer.send(topic = t,value=line)
                send_count += 1
                if(send_count == batch_size):
                    send_count = 0
                    time.sleep(window)
