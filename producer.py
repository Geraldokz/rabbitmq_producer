import os
import json

import pika
import pika.exceptions
from dotenv import load_dotenv

from exeptions import ProducerException

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
RABBITMQ_EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')


def push_to_queue(data: dict, retries_count: int) -> None:

    if type(data) != dict:
        raise TypeError(f'expect dict')

    channel = _connect_to_rabbitmq()
    _declare_rabbitmq_queue(channel)

    for retry in range(retries_count):
        try:
            channel.basic_publish(
                exchange='',
                routing_key=RABBITMQ_QUEUE,
                body=json.dumps(data, indent=2)
            )
            break
        except pika.exceptions.UnroutableError:
            continue

    channel.close()


def _connect_to_rabbitmq() -> pika.BlockingConnection.channel:
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        return connection.channel()
    except:
        raise ProducerException('error while connecting to rabbitmq')


def _declare_rabbitmq_queue(channel: pika.BlockingConnection.channel) -> None:
    try:
        channel.queue_declare(queue=RABBITMQ_QUEUE)
    except:
        raise ProducerException('error while declaring rabbitmq queue')


if __name__ == '__main__':
    data_to_push = {
        'header1': 'abc346',
        'header2': 'lkj1111',
        'header3': 'zxc',
    }

    push_to_queue(data_to_push, retries_count=10)
