import os
import sys

import pika

from producer import RABBITMQ_HOST, RABBITMQ_VIRTUAL_HOST, RABBITMQ_KEY


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        virtual_host=RABBITMQ_VIRTUAL_HOST)
    )
    channel = connection.channel()

    channel.queue_declare(queue=RABBITMQ_KEY)

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)

    channel.basic_consume(queue=RABBITMQ_KEY, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
