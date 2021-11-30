import time
from functools import wraps
from typing import Union


def retry(exception: type(Exception), retries: int, delay: Union[int, float], delay_increase: int, max_delay: int):
    """
    Декоратор, вызывающий повторное выполнение функции при возникновении
    исключения

    :param exception: отлавливаемое исключение
    :param retries: кол-во попыток
    :param delay: задержка выполнения функции
    :param delay_increase: коэффициент увеличения задержки
    :param max_delay: максимально допустимое время задержки
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
                    _delay = _delay * delay_increase if _delay * delay_increase < max_delay else max_delay
        return func_with_retries
    return retry_decorator
