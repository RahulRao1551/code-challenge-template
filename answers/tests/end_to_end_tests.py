import unittest
import requests


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host = "http://127.0.0.1:5000"

    def test_weather_data_simple(self):
        response = requests.get(f"{self.host}/api/weather")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("payload", data)
        self.assertGreater(data['payload']['count'], 0)
        self.assertEqual(len(data['payload']['results']), 10)
        self.assertEqual(data['payload']['offset'], 0)

    def test_weather_data_pagination(self):
        for date in ('1996-01-01', '1997-01-01', '1998-01-01'):
            for offset in range(0, 40, 20):
                for limit in range(10, 30, 10):
                    response = requests.get(f'{self.host}/api/weather?date={date}&offset={offset}&limit={limit}')
                    self.assertEqual(response.status_code, 200)
                    data = response.json()
                    self.assertEqual(len(data['payload']['results']), limit)

    def test_yield_data_simple(self):
        resp = requests.get(f'{self.host}/api/yield')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("payload", data)
        self.assertGreater(data['payload']['count'], 0)
        self.assertEqual(len(data['payload']['results']), 10)
        self.assertEqual(data['payload']['offset'], 0)

    def test_yield_data_pagination(self):
        for limit in range(10, 30, 10):
            resp = requests.get(f'{self.host}/api/yield?year=2000&offset=0&limit={limit}')
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(len(resp.json()['payload']['results']), 1)

    def test_stats_simple(self):
        resp = requests.get(f'{self.host}/api/weather/stats')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("payload", data)
        self.assertGreater(data['payload']['count'], 0)
        self.assertEqual(len(data['payload']['results']), 10)
        self.assertEqual(data['payload']['offset'], 0)

    def test_stats_pagination(self):
        for year in (1987, 1988):
            for offset in range(0, 40, 20):
                for limit in range(10, 30, 10):
                    resp = requests.get(f'{self.host}/api/weather/stats?year={year}&offset={offset}&limit={limit}')
                    json_resp = resp.json()
                    self.assertEqual(len(json_resp['payload']['results']), limit)


if __name__ == '__main__':
    unittest.main()
