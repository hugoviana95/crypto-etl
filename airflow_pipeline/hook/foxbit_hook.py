from airflow.providers.http.hooks.http import HttpHook
import requests
from urllib.parse import urlencode

class FoxbitHook(HttpHook):
# Documentation: https://docs.foxbit.com.br/rest/v3/#tag/Market-Data/operation/MarketsController_findCandlesticks

    def __init__(self, market_symbol, interval, start_time, end_time, limit):
        self.conn_id = 'foxbit_default'
        self.market_symbol = market_symbol
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.limit = limit
        super().__init__(http_conn_id=self.conn_id)

        # Verifique se self.base_url foi configurado corretamente
        if not self.base_url:
            connection = self.get_connection(self.conn_id)
            self.base_url = connection.host
            # Adicione o esquema se estiver faltando
            if not self.base_url.startswith('http'):
                self.base_url = f"https://{self.base_url}"


    def create_url(self):
        params = {
            'interval': self.interval,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'limit': self.limit
        }
        url = f"{self.base_url}/rest/v3/markets/{self.market_symbol}/candlesticks?{urlencode(params)}"
        return url
    
    def connect_to_endpoint(self, url, session):
        self.log.info(f"URL: {url}")
        request = requests.Request('GET', url)
        prep = session.prepare_request(request)
        return self.run_and_check(session, prep, {})
    
    def run(self):
        url = self.create_url()
        session = self.get_conn()
        return self.connect_to_endpoint(url, session)



