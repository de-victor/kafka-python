import json
import random
import threading
import time

from kafka import KafkaConsumer, KafkaProducer

class EnumTopic():
    le_news = 'le_news'
    like_no_like = 'like_no_like'

class CountAll():
    count_news = 0
    count_like = 0
    count_not_like = 0

class Producer(threading.Thread):
    topic = ''

    def __init__(self, topic):
        super(Producer, self).__init__()
        self.topic = topic
    
    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            id_ = random.randint(1, 10)
            data = {}

            if self.topic == EnumTopic.le_news:
                message = [id_]
            elif self.topic == EnumTopic.like_no_like:
                tente = random.randint(1, 500)
                if(tente % 2):
                    message = [id_, {'like': 1, 'no_like': 0}]
                else:
                    message = [id_, {'like': 0, 'no_like': 1}]

            producer.send(self.topic, message)
            time.sleep(random.randint(0, 5))


class Consumer(threading.Thread):
    topic = ''

    def __init__(self, topic):
        super(Consumer, self).__init__()
        self.topic = topic

    def run(self):
        stream = KafkaConsumer(bootstrap_servers='localhost:9092',
                               auto_offset_reset='latest')
        stream.subscribe([self.topic])
        for tuple in stream:
            if self.topic == EnumTopic.le_news:
                CountAll.count_news += 1
                print 'Noticias lidas: ' + str(CountAll.count_news)
            if self.topic == EnumTopic.like_no_like:
                linha = json.loads(tuple[6])
                contador = linha[1]
                CountAll.count_like += contador['like']
                CountAll.count_not_like += contador['no_like']
                print 'total curtidas: '+ str(CountAll.count_like) + ' total nao curtidas: ' + str(CountAll.count_not_like)

if __name__ == '__main__':
    threads = [
        Producer(EnumTopic.le_news),
        Producer(EnumTopic.like_no_like),
        Consumer(EnumTopic.le_news),
        Consumer(EnumTopic.like_no_like)
    ]

    for t in threads:
        t.start()
