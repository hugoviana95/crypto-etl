from airflow.providers.http.hooks.http import HttpHook
import requests
from urllib.parse import urlencode

class BinanceHook(HttpHook):

    def __init__(self, start_time, end_time, symbol, interval):
        self.start_time = start_time
        self.end_time = end_time
        self.symbol = symbol
        self.interval = interval
        self.conn_id = 'binance_default'
        super().__init__(http_conn_id=self.conn_id)


    def create_url(self, **kwargs):
        """
        Params description:

        Name	    Type	Mandatory	Description
        symbol	    STRING	YES	
        interval	ENUM	YES	
        startTime	LONG	NO	
        endTime	    LONG	NO	
        timeZone	STRING	NO	        Default: 0 (UTC)
        limit	    INT	    NO	        Default 500; max 1000.

        """

        url = f"{self.base_url}/api/v3/klines"
        params = {
            'symbol': self.symbol, 
            'interval': self.interval, 
            'startTime': self.start_time, 
            'endTime': self.end_time,
            'timeZone': '-3'
        }
        params.update(kwargs)
        url = f"{url}?{urlencode(params)}"
        return url


    def connect_to_endpoint(self, url, session):
        # See documentation at https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        request = requests.Request("GET", url)
        session = self.get_conn()
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})


    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        return self.connect_to_endpoint(url_raw, session)

