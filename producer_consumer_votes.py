import json
import random
import threading
import time

from kafka import KafkaConsumer, KafkaProducer

class Candidates():
    candidates = [{'id': 10, 'name': 'Bolsorado Roe', 'party':'LSP', 'votes':0},
                  {'id': 11, 'name': 'Hajda Daja', 'party':'TPI', 'votes':0},
                  {'id': 12, 'name': 'Dacitrolo Loto', 'party':'VUX', 'votes':0}]

class CountAll():
    total_votes = 0

class VoteProducer(threading.Thread):
    
    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            vote = random.randint(10, 12)
            data = {'vote': vote}
            message = [vote, data]

            producer.send('votes', message)
            time.sleep(random.randint(0, 2))


class VoteConsumer(threading.Thread):

    def run(self):
        stream = KafkaConsumer(bootstrap_servers='localhost:9092',
                               auto_offset_reset='latest')
        stream.subscribe(['votes'])

        for tuple in stream:
            linha = json.loads(tuple[6])
            value = linha[1]
            CountAll.total_votes += 1
            candidate = [x for x in Candidates.candidates if x['id'] == value['vote']]
            candidate[0]['votes'] += 1
            
            print 'total votos: ' + str(CountAll.total_votes)
            
            for cand in Candidates.candidates:
                print 'Cadidato : ' + cand['name'] + ' votos: ' + str(cand['votes'])

if __name__ == '__main__':
    threads = [
        VoteProducer(),
        VoteProducer(),
        VoteProducer(),
        VoteConsumer()
    ]

    for t in threads:
        t.start()
