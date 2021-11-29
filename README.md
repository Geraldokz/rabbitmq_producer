# rabbitmq_producer
***Python module for sending data to rabbitmq***

Для начала работы необходимо создать экзмеляр класса `RabbitMQProducer` из модуля `producer`

При создании экземпляра класса будет создана очередь и отдельный процесс, который прослушивает очередь и отправляет данные в rabbitmq.

Отправка сообщения в очередь осуществляется черерз метод `push_message_to_queue` класса `RabbitMQProducer`. В данный метод передаются следующие аргументы:

`data` - словарь, который необходимо передать в rabbitmq

`retry_exception` - исключение, при котором будет срабатывать retry политика

`retries_count` - количество попыток выполнения функции

`retry_delay` - задержка повторного выполнения функции

`retry_delay_increase` - коэффициент увеличения задержки

Пример

~~~~
from producer import RabbitMQProducer


if __name__ == '__main__':
  data_to_push = {
    'header1': '123',
    'header2': 'abc',
  }
  
  rabbitmq_producer = RabbitMQProducer()
  
~~~~
