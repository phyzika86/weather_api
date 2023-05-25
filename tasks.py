from external.client import YandexWeatherAPI
from utils import get_url_by_city_name, CITIES
import logging
from concurrent.futures import ThreadPoolExecutor
from external.analyzer import analyze_json
from multiprocessing import Pool, Manager, cpu_count
import json
from dataclasses import dataclass
from typing import Dict
from queue import Queue
from time import sleep
from config import PATH_TO_OUTPUT
from api_exception import ApiException, URLNotFoundException


@dataclass
class DataFetchingTask:
    cities: Dict

    def __init__(self, cities):
        self.cities = cities

    @staticmethod
    def fetch_url(city_name: str) -> dict:
        url = get_url_by_city_name(city_name)

        logging.info('Начал получать данные погоды для города: %s (url: %s)', city_name, url)
        try:
            resp = YandexWeatherAPI.get_forecasting(url)
            sleep(0.1)
        except ApiException as e:
            resp = {}
            logging.exception('Для города: %s (url: %s) произошла ошибка в данных: %s', city_name, url, e)
        except URLNotFoundException as e:
            resp = {}
            logging.exception('Для города: %s (url: %s) произошла ошибка c url: %s', city_name, url, e)
        except Exception as e:
            resp = {}
            logging.exception('Для города: %s (url: %s) произошла ошибка: %s', city_name, url, e)

        return {city_name: resp}

    @staticmethod
    def get_data(cities: dict):
        cities = list(cities.keys())
        fetch_url = DataFetchingTask.fetch_url
        with ThreadPoolExecutor() as pool:
            res = pool.map(fetch_url, cities)

        return res


@dataclass
class DataCalculationTask:
    result: list
    queue: Queue

    def __init__(self, queue):
        self.queue = queue
        self.result = []

    def run(self):
        logging.info('Начал вычислять среднюю температуру для всех данных городов')
        with Pool(cpu_count() - 1) as pool:
            while queue.qsize():
                res = pool.apply_async(analyze_json, [queue])
                res = res.get()
                self.result.append(self.calculate_avg(res['days'], res['city']) if res else res)

        logging.info('Закончил вычислять среднюю температуру для всех данных городов')

    def calculate_avg(self, days: list, city: str) -> dict:
        temp_avg_main = 0
        hours_avg_main = 0

        count_temp = 0
        count_hours = 0
        for day in days:
            temp_avg = day.get('temp_avg')
            if not (temp_avg is None):
                temp_avg_main += temp_avg
                count_temp += 1

            if not (temp_avg is None):
                hours_avg_main += day.get('relevant_cond_hours')
                count_hours += 1

        temp_avg_main = temp_avg_main / count_temp if count_temp else temp_avg_main
        hours_avg_main = hours_avg_main / count_hours if count_hours else hours_avg_main

        return {'temperature_avg': temp_avg_main, 'hours_without_hours': hours_avg_main, 'days': days, 'city': city}


class DataAggregationTask:
    def __init__(self):
        pass

    @staticmethod
    def write_in_file(data: list):
        with open(PATH_TO_OUTPUT, mode="w") as file:
            formatted_data = json.dumps(data, indent=2)
            file.write(formatted_data)


@dataclass
class DataAnalyzingTask:
    result: list
    best_city: dict

    def __init__(self):
        self.result = []
        self.best_city = {}

    def get_analyze(self, queue):
        data = DataCalculationTask(queue)
        data.run()
        calculate_data = data.result
        self.result = sorted(calculate_data,
                             key=lambda x: (-x.get('temperature_avg', -1000), -x.get('hours_without_hours', 0)))
        self.set_rating()
        self.best_city = self.result[0]

    def set_rating(self):
        for i in range(len(self.result)):
            self.result[i]['rating'] = i + 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(name)s - %(levelname)s - %(message)s')
    logging.info('Начинаю получать данные')
    weathers = DataFetchingTask(CITIES)

    m = Manager()
    queue = m.Queue()
    res = []
    cities_with_data = weathers.cities
    for weather in weathers.get_data(cities_with_data):
        queue.put(weather)
    logging.info('Закончил получать данные')
    analyze_data = DataAnalyzingTask()
    analyze_data.get_analyze(queue)
    logging.info('Произвожу  запись агрегированных данныхв файл')
    DataAggregationTask.write_in_file(analyze_data.result)
    logging.info('Закончил запись агрегированных данныхв файл')
    logging.info(f'Лучший город {analyze_data.best_city.get("city")}')
