from producer import RabbitMQProducer


if __name__ == '__main__':
    data_to_push = {
        'header1': '123',
        'header2': 'abc',
        'header3': 'zxc',
    }

    rabbitmq_producer = RabbitMQProducer()

    while True:
        rabbitmq_producer.push_message_to_queue(data_to_push, retries_count=2)
