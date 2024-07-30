from airflow.providers.http.hooks.http import HttpHook
import requests
from urllib.parse import urlencode

class NovadaxHook(HttpHook):
# Documentation: https://doc.novadax.com/pt-BR/#obter-dados-de-candlestick

    def __init__(self, from_time, to_time, symbol, unit):
        self.conn_id = 'novadax_default'
        self.symbol = symbol
        self.unit = unit
        self.from_time = from_time
        self.to_time = to_time
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
            'symbol': self.symbol,
            'unit':self.unit,
            'from':self.from_time,
            'to':self.to_time
        }
        url = f'{self.base_url}/v1/market/kline/history?{urlencode(params)}'
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
    

if __name__ == '__main__':
    from airflow import DAG

    with DAG('teste_dag'):
        r = NovadaxHook(1722083200, 1722083699, 'BTC_BRL', 'FIVE_MIN').run()
        print(r.json())