import os
import json

import pika
import pika.exceptions
from dotenv import load_dotenv

from exeptions import ProducerException
from retry_policy import retry

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
RABBITMQ_EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')


def push_to_queue(data: dict) -> None:
    """Отправялет данные в очередь rabbitmq"""
    channel = _connect_to_rabbitmq()
    _declare_rabbitmq_queue(channel)

    channel.basic_publish(
        exchange=RABBITMQ_EXCHANGE,
        routing_key=RABBITMQ_QUEUE,
        body=json.dumps(data, indent=2)
    )

    channel.close()


def _connect_to_rabbitmq() -> pika.BlockingConnection.channel:
    """Инициализирует подключение к rabbitmq"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        return connection.channel()
    except Exception as e:
        raise ProducerException(f'error while connecting to rabbitmq, traceback below\n{str(e)}')


def _declare_rabbitmq_queue(channel: pika.BlockingConnection.channel) -> None:
    """Создает очередь в rabbitmq"""
    try:
        channel.queue_declare(queue=RABBITMQ_QUEUE)
    except Exception as e:
        raise ProducerException(f'error while declaring rabbitmq queue, traceback below\n{str(e)}')


if __name__ == '__main__':
    data_to_push = {
        'header1': '123',
        'header2': 'abc',
        'header3': 'zxc',
    }

    @retry(ProducerException, retries=2)
    def send_low_severity_event(data: dict) -> None:
        push_to_queue(data)


    send_low_severity_event(data_to_push)
