import time
from functools import wraps


def retry(exception=Exception, retries=5, delay=0.5, delay_increase=2):
    """
    Декоратор, вызывающий повторное выполнение функции при возникновении
    исключения

    :param exception: отлавливаемое исключение
    :param retries: кол-во попыток
    :param delay: задержка выполнения функции
    :param delay_increase: коэффициент увеличения задержки
    :return:
    """
    def retry_decorator(f):
        @wraps(f)
        def func_with_retries(*args, **kwargs):
            _retries, _delay = retries + 1, delay

            while _retries > 1:
                try:
                    return f(*args, **kwargs)
                except exception as e:
                    _retries -= 1

                    if _retries == 1:
                        break

                    time.sleep(_delay)
                    _delay *= delay_increase
        return func_with_retries
    return retry_decorator
