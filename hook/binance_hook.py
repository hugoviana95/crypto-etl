from airflow.providers.http.hooks.http import HttpHook
import requests
from urllib.parse import urlencode

class BinanceHook(HttpHook):
# See the API documentation at https://developers.binance.com/docs/binance-spot-api-docs/rest-api#klinecandlestick-data

    def __init__(self, start_time, end_time, symbol, interval):
        self.start_time = start_time
        self.end_time = end_time
        self.symbol = symbol
        self.interval = interval
        self.conn_id = 'binance_default'
        super().__init__(http_conn_id=self.conn_id)


    def create_url(self):
        url = f"{self.base_url}/api/v3/klines"
        params = {
            'symbol': self.symbol,
            'interval': self.interval,
            'startTime': self.start_time,
            'endTime': self.end_time,
        }
        url = f"{url}?{urlencode(params)}"
        print(self.base_url)
        return url


    def connect_to_endpoint(self, url, session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})


    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        return self.connect_to_endpoint(url_raw, session)

