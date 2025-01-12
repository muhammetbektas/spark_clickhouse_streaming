from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
import random
default_args = {
    'owner':'admin',
    'start_date':datetime(2025,1,12,9,00)
}

def get_data():
    import json
    import requests
    api_key=os.getenv('NEWS_API_KEY')
    params= {"q": "*", "apiKey": api_key}
    res = requests.get('https://newsapi.org/v2/everything',params=params)
    res = res.json()
    res = res['articles'][0]
    return res

def format_data(res):
    data = {}
    data['source_id'] = str(uuid.uuid4())
    data['source_name'] = res['source']['name']
    data['author'] = res['author'] if res['author']!=None else 'nodata'
    data['title'] = res['title']
    data['description'] = res['description']
    data['url'] = res['url']
    data['urlToImage'] = res['urlToImage'] if res['urlToImage']!=None else 'nodata'
    data['publishedAt'] = res['publishedAt']
    data['content'] = res['content']
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging


    producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 min
            break
        try:
            data = get_data()
            formatted_data = format_data(data)
            producer.send('news_data',json.dumps(formatted_data).encode('utf-8'))
        except Exception as e:
            print(f'An error occured: {e} ')
            continue

with DAG('newsapi_2kafka',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'pull_data_from_newsapi',
        python_callable = stream_data
    )