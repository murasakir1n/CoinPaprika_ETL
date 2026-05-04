from datetime import datetime, timedelta, UTC
import json
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import create_engine
import psycopg2


# Настройка для все задач в Dag

default_args = {
    'owner': 'Sergey',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='crypto_tickers',
    description='Fetching crypto tickers from Coinpaprika and load to PostgreSQL',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['crypto', 'etl'],
)
def crypto_etl():


    api = Variable.get('API')


    @task()
    def extract() -> list:

        try:

            response = requests.get(api)

            response.raise_for_status()

            data = response.json()

            return data


        except Exception as e:
            raise f'Api is not working. Error: {e}'


    @task()
    def transform(data: list) -> list:
        processed = []
        for coin in data:
            processed.append({
                'id': coin['id'],
                'rank': coin['rank'],
                'name': coin['name'],
                'symbol': coin['symbol'],
                'max_supply': coin['max_supply'],
                'total_supply': coin['total_supply'],
                'price': coin['quotes']['USD']['price'],
                'market_cap': coin['quotes']['USD']['market_cap'],
                'volume_24h': coin['quotes']['USD']['volume_24h'],
                'percent_change_24h': coin['quotes']['USD']['percent_change_24h'],
                'percent_change_7d': coin['quotes']['USD']['percent_change_7d'],
                'last_updated': coin['last_updated'],
            })

            data = pd.DataFrame(processed)

        return data

    @task()
    def load(data: list) -> None:

        conn = Variable.get('DB')
        engine = create_engine(conn)

        data = pd.DataFrame(data)


        data.to_sql(
            name='crypto_tickers',
            con=engine,
            if_exists='append',
            index=False,
        )

        print(f'Загружено монет: {len(data)}')

    raw = extract()
    clean = transform(raw)
    load(clean)

crypto_etl()