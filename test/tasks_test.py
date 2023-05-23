import unittest
from tasks import DataCalculationTask, DataFetchingTask
from queue import Queue
from utils import CITIES
from external.analyzer import analyze_json


class TasksTestCase(unittest.TestCase):

    def test_calculate_avg(self):
        """
        Тест функции, которая считает среднюю температуру и среднее релевантное число дней без осадков
        за количество дней, возращенных через API
        """
        q = Queue()
        days_1 = [{'date': '2022-05-26', 'hours_start': 9, 'hours_end': 19, 'hours_count': 11, 'temp_avg': 2,
                   'relevant_cond_hours': 7},
                  {'date': '2022-05-27', 'hours_start': 9, 'hours_end': 19, 'hours_count': 11, 'temp_avg': 4,
                   'relevant_cond_hours': 0},
                  {'date': '2022-05-28', 'hours_start': 9, 'hours_end': 19, 'hours_count': 11, 'temp_avg': 2,
                   'relevant_cond_hours': 0},
                  {'date': '2022-05-29', 'hours_start': 9, 'hours_end': 9, 'hours_count': 1, 'temp_avg': 8,
                   'relevant_cond_hours': 1},
                  {'date': '2022-05-30', 'hours_start': None, 'hours_end': None, 'hours_count': 0, 'temp_avg': None,
                   'relevant_cond_hours': 0}]

        days_2 = [{'date': '2022-05-26', 'hours_start': 9, 'hours_end': 19, 'hours_count': 11, 'temp_avg': None,
                   'relevant_cond_hours': 7},
                  {'date': '2022-05-27', 'hours_start': 9, 'hours_end': 19, 'hours_count': 11, 'temp_avg': None,
                   'relevant_cond_hours': 0},
                  {'date': '2022-05-28', 'hours_start': 9, 'hours_end': 19, 'hours_count': 11, 'temp_avg': None,
                   'relevant_cond_hours': 0},
                  {'date': '2022-05-29', 'hours_start': 9, 'hours_end': 9, 'hours_count': 1, 'temp_avg': None,
                   'relevant_cond_hours': 1},
                  {'date': '2022-05-30', 'hours_start': None, 'hours_end': None, 'hours_count': 0, 'temp_avg': None,
                   'relevant_cond_hours': 0}]

        city = 'MOSCOW'
        data = DataCalculationTask(q)
        res_1 = data.calculate_avg(days_1, city)
        test_res = {'temperature_avg': 4, 'hours_without_hours': 2, 'days': days_1, 'city': city}
        self.assertEqual(res_1, test_res)

        res_2 = data.calculate_avg(days_2, city)
        test_res_2 = {'temperature_avg': 0, 'hours_without_hours': 0, 'days': days_2, 'city': city}
        self.assertEqual(res_2, test_res_2)

    def test_data_fetching_task(self):
        """
        Тест на проверку того, что результат при работе с потоками совпадает с результатом однопоточного приложения
        """
        weathers = DataFetchingTask(CITIES)
        weathers = weathers.get_data(weathers.cities)
        q = Queue()
        for weather in weathers:
            city = list(weather.keys())[0]
            res = DataFetchingTask.fetch_url(city)[city]
            q.put({city: res})
            q.put({city: weather[city]})
            self.assertEqual(analyze_json(q), analyze_json(q))


if __name__ == '__main__':
    unittest.main()
