import json
import logging
from http import HTTPStatus
from urllib.request import urlopen
from urllib.error import HTTPError
from json import JSONDecodeError
from api_exception import ApiException, URLNotFoundException

ERR_MESSAGE_TEMPLATE = "Unexpected error: {error}"

logger = logging.getLogger()


class YandexWeatherAPI:
    """
    Base class for requests
    """

    @staticmethod
    def __do_req(url: str) -> str:
        """Base request method"""
        try:
            with urlopen(url) as response:
                resp_body = response.read().decode("utf-8")
                data = json.loads(resp_body)
            if response.status != HTTPStatus.OK:
                raise Exception(
                    "Error during execute request. {}: {}".format(
                        resp_body.status, resp_body.reason
                    )
                )
            return data
        except Exception as ex:
            logger.error(ex)
            if issubclass(ex.__class__, JSONDecodeError):
                raise ApiException(
                    f'Ошибка в данных json от сервера {ERR_MESSAGE_TEMPLATE.format(error=ex)}')
            if issubclass(ex.__class__, HTTPError):
                raise URLNotFoundException(f'Возможно неверный url {ERR_MESSAGE_TEMPLATE.format(error=ex)}')
            else:
                raise Exception(ERR_MESSAGE_TEMPLATE.format(error=ex))

    @staticmethod
    def get_forecasting(url: str):
        """
        :param url: url_to_json_data as str
        :return: response data as json
        """
        return YandexWeatherAPI.__do_req(url)
