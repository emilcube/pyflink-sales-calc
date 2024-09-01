import argparse
import atexit
import json
import logging
import random
import time
import sys
from confluent_kafka import Producer

logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("sales_producer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()

SELLERS = ['LNK', 'OMA', 'KC', 'DEN']

class ProducerCallback:
    def __init__(self, record, log_success=False):
        self.record = record
        self.log_success = log_success

    def __call__(self, err, msg):
        if err:
            logger.error(f"Error producing record {self.record}: {err}")
        elif self.log_success:
            logger.info(f"Produced {self.record} to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

def main(args):
    logger.info('Starting sales producer')
    conf = {
        'bootstrap.servers': args.bootstrap_server,
        'linger.ms': 200,
        'client.id': 'sales-1',
        'partitioner': 'murmur2_random'
    }

    producer = Producer(conf)
    atexit.register(producer.flush)

    i = 1
    while True:
        is_tenth = i % 10 == 0

        sales = {
            'seller_id': random.choice(SELLERS),
            'amount_usd': random.uniform(100, 1000),  # use uniform for more realistic floating-point values
            'sale_ts': int(time.time() * 1000)
        }

        producer.produce(
            topic=args.topic,
            value=json.dumps(sales),
            on_delivery=ProducerCallback(sales, log_success=is_tenth)
        )

        if is_tenth:
            producer.poll(1)
            time.sleep(30) # 5
            i = 0  # reset i to 0 to prevent it from growing indefinitely

        i += 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='sales-usd')
    args = parser.parse_args()
    main(args)
