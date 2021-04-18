import logging
import json
from datetime import datetime, timedelta

import requests
from retry import retry

import azure.functions as func


class RateLimitError(Exception):
    pass


class CoinGecko():
    base_url = 'https://api.coingecko.com/api/v3'
    today = datetime.utcnow().date()
    start_datestamp = (today - timedelta(90)).strftime('%d-%m-%Y')

    def __init__(self, request):
        self.request = request
        self.state = self.request['state']
        self.secrets = self.request['secrets']
        self.coin_ids = self.state.get('coin_ids', [])
        self.had_more = self.state.get('has_more', False)
        self.target_datestamp = self.state.get('next_date', self.start_datestamp)
        self.target_date = datetime.strptime(self.target_datestamp, '%d-%m-%Y').date()
        
    @retry(tries=3, delay=70)
    def get_markets(self):
        r = requests.get(self.base_url + '/coins/markets',
            params = {'vs_currency': 'usd', 'per_page': 100, 'page': 1}
        )
        if r.status_code == 429:
            raise RateLimitError()
        markets = r.json()
        return markets

    @retry(tries=3, delay=70)
    def get_coin_history(self, coin_id):
        r = requests.get(self.base_url + '/coins/{}/history'.format(coin_id),
            params = {'date': self.target_datestamp}
        )
        if r.status_code == 429:
            raise RateLimitError()
        history = r.json()
        history['date'] = self.target_date.isoformat()
        return history

    def get_history(self):
        self.history = []
        for coin_id in self.coin_ids:
            coin_history = self.get_coin_history(coin_id)
            self.history.append(coin_history)

        if self.target_date < self.today:
            self.next_date = self.target_date + timedelta(1)
            self.has_more = True
        else:
            self.next_date = self.target_date
            self.has_more = False

    def get_response(self):
        if self.had_more:
            self.markets = []
        else: 
            self.markets = self.get_markets()
            for market in self.markets:
                if market['id'] not in self.coin_ids:
                    self.coin_ids.append(market['id'])
        self.get_history()

        response = {
            'state': {
                'coin_ids': self.coin_ids,
                'next_date': self.next_date.strftime('%d-%m-%Y'),
                'has_more': self.has_more
            },
            'insert': {
                'market': self.markets,
                'history': self.history
            },
            'delete': {
                'market': [],
                'history': []
            },
            'schema': {
                'market': {'primary_key': ['id']},
                'history': {'primary_key': ['id', 'date']}
            },
            'hasMore': self.has_more
        }
        return response


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    coin_gecko = CoinGecko(req.get_json())
    response = coin_gecko.get_response()
    return func.HttpResponse(json.dumps(response))