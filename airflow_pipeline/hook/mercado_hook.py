from airflow.providers.http.hooks.http import HttpHook
import requests
from urllib.parse import urlencode

class MercadoHook(HttpHook):
# See the API documentation at https://api.mercadobitcoin.net/api/v4/docs#tag/Public-Data/paths/~1candles/get

    def __init__(self, symbol, resolution, to_time, from_time):
        self.conn_id = 'mercado_default'
        self.symbol = symbol
        self.resolution = resolution
        self.to_time = to_time
        self.from_time = from_time
        super().__init__(http_conn_id=self.conn_id)


    def create_url(self):
        params = {
            'symbol': self.symbol,
            'resolution': self.resolution,
            'to': self.to_time,
            'from': self.from_time,
            # 'countback': 1,
        }
        url = f"{self.base_url}/candles?{urlencode(params)}"
        return url
    
    def connect_to_endpoint(self, url, session):
        request = requests.Request('GET', url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})
    
    def run(self):
        session = self.get_conn()
        url = self.create_url()
        return self.connect_to_endpoint(url, session)