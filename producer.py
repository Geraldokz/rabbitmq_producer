import os
import json
from typing import NamedTuple, Union
from multiprocessing import Process, Queue

import pika
import pika.exceptions
from dotenv import load_dotenv

from exeptions import ProducerException
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

    def __init__(self):
        self.queue = Queue()
        self._init_queue_listener()

    def push_message_to_queue(self, data: dict, retry_exception=Exception, retries_count=5, retry_delay=0.5,
                              retry_delay_increase=2) -> None:
        """Формирует именнованный кортеж с принятым словарем и retry политикамии и отпраляет его в очередь"""
        if isinstance(data, dict):
            queue_message = self.QueueMessage(
                data=json.dumps(data, indent=2),
                retry_exception=retry_exception,
                retries_count=retries_count,
                retry_delay=retry_delay,
                retry_delay_increase=retry_delay_increase
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
        """
        channel = _connect_to_rabbitmq()
        _declare_rabbitmq_queue(channel)

        while True:
            queue_message = self.queue.get()
            _push_to_rabbitmq(queue_message, channel)


def _push_to_rabbitmq(message: RabbitMQProducer.QueueMessage, channel: pika.BlockingConnection.channel) -> None:
    """
    Декорирует функцию send_message полученными retry политиками из сообщения
    и запускает отправку данных в rabbitmq
    """
    @retry(exception=message.retry_exception, retries=message.retries_count,
           delay=message.retry_delay, delay_increase=message.retry_delay_increase)
    def send_message():
        channel.basic_publish(
            exchange=RABBITMQ_EXCHANGE,
            routing_key=RABBITMQ_KEY,
            body=message.data
        )

    send_message()


def _connect_to_rabbitmq() -> pika.BlockingConnection.channel:
    """Инициализирует подключение к rabbitmq"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            virtual_host=RABBITMQ_VIRTUAL_HOST)
        )
        return connection.channel()
    except Exception as e:
        raise ProducerException(f'error while connecting to rabbitmq, traceback below\n{str(e)}')


def _declare_rabbitmq_queue(channel: pika.BlockingConnection.channel) -> None:
    """Создает очередь в rabbitmq"""
    try:
        channel.queue_declare(queue=RABBITMQ_KEY)
    except Exception as e:
        raise ProducerException(f'error while declaring rabbitmq queue, traceback below\n{str(e)}')
