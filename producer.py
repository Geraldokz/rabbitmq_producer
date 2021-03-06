import os
import json
from typing import NamedTuple, Union
from multiprocessing import Process, Queue

import pika
from dotenv import load_dotenv
from pika.exceptions import ConnectionClosed, ChannelClosed

from retry_policy import retry

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_VIRTUAL_HOST = os.getenv('RABBITMQ_VIRTUAL_HOST')
RABBITMQ_KEY = os.getenv('RABBITMQ_KEY')
RABBITMQ_EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')


class RabbitMQProducer:
    """Класс для отправки данных в rabbitmq

    При создании экземпляра класса создается очередь, в которую отправляются данные
    и вызывается метод _init_queue_listener, создающий отдельный процесс
    для прослушивания очереди и отправки данных в rabbitmq

    Метод push_message_to_queue отправляет данные в очередь
    """

    class QueueMessage(NamedTuple):
        data: str
        retry_exception: type(Exception)
        retries_count: int
        retry_delay: Union[int, float]
        retry_delay_increase: int
        retry_max_delay: int

    def __init__(self):
        self.queue = Queue()
        self._init_queue_listener()

    def push_message_to_queue(self, data: dict, retry_exception=Exception, retries_count=5, retry_delay=0.5,
                              retry_delay_increase=2, retry_max_delay=30) -> None:
        """Формирует именнованный кортеж с принятым словарем и retry политикамии и отпраляет его в очередь"""
        if isinstance(data, dict):
            queue_message = self.QueueMessage(
                data=json.dumps(data, indent=2),
                retry_exception=retry_exception,
                retries_count=retries_count,
                retry_delay=retry_delay,
                retry_delay_increase=retry_delay_increase,
                retry_max_delay=retry_max_delay
            )
            self.queue.put(queue_message)

    def _init_queue_listener(self):
        """
        Запускает отдельный процесс прослушивания очереди на наличие новых данных
        и отправки их в rabbitmq
        """
        queue_listener = Process(target=self._listen_to_queue)
        queue_listener.start()

    def _listen_to_queue(self) -> None:
        """
        Получает сообщение из очереди в виде именованного кортежа QueueMessage
        и передает его в метод _push_to_rabbitmq

        В случае возникновения проблем с подключением к rabbitmq, запускается
        процесс переподключения
        """
        channel = _connect_to_rabbitmq()

        while True:
            queue_message = self.queue.get()
            try:
                _push_to_rabbitmq(queue_message, channel)
            except (ConnectionClosed, ChannelClosed):
                channel = _reconnect_to_rabbitmq()
                _push_to_rabbitmq(queue_message, channel)


def _push_to_rabbitmq(message: RabbitMQProducer.QueueMessage, channel: pika.BlockingConnection.channel) -> None:
    """
    Декорирует функцию send_message полученными retry политиками из сообщения
    и запускает отправку данных в rabbitmq
    """
    @retry(exception=message.retry_exception, retries=message.retries_count,
           delay=message.retry_delay, delay_increase=message.retry_delay_increase, max_delay=message.retry_max_delay)
    def send_message():
        channel.basic_publish(
            exchange=RABBITMQ_EXCHANGE,
            routing_key=RABBITMQ_KEY,
            body=message.data
        )

    send_message()


def _connect_to_rabbitmq() -> pika.BlockingConnection.channel:
    """Инициализирует подключение к rabbitmq"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        virtual_host=RABBITMQ_VIRTUAL_HOST)
    )

    channel = connection.channel()
    channel.queue_declare(RABBITMQ_KEY)

    return channel


def _reconnect_to_rabbitmq() -> pika.BlockingConnection.channel:
    """Запускает процесс переподключения к rabbitmq"""
    while True:
        try:
            channel = _connect_to_rabbitmq()
            return channel
        except Exception:
            continue
