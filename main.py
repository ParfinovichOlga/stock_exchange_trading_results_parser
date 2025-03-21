import requests
import threading
import os
import pandas as pd
import numpy as np
import datetime as dt
from link_collection import LinkCollection
from concurrent.futures import ThreadPoolExecutor
from orm import create_tables, insert_data_pull_to_db


START_URL = "https://spimex.com/markets/oil_products/trades/results/"
START_FROM = dt.datetime(2023, 1, 1).date()

create_tables()

link_builder = LinkCollection(START_FROM, START_URL)
links = link_builder.grab_links()
links.reverse()

thread_local = threading.local()


def get_session_for_thread():
    """Return requests module session for downloading files."""
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
    return thread_local.session


def download_file(url):
    """Download file from url to server."""
    requests_session = get_session_for_thread()
    query_params = {'downloadformat': 'xlsx'}
    with requests_session.get(url, params=query_params, stream=True) as response:
        date = LinkCollection.get_file_date(url)
        name = f'{date}.xlsx'

        with open(name, mode='wb') as file:
            for chunk in response.iter_content():
                file.write(chunk)
        return name, date


def retrieve_requested_data_from_file(file_name, date):
    """Retrieve requested data from file."""
    pre_data = pd.read_excel(file_name, usecols='B', nrows=20)

    row, col = np.where(pre_data == 'Единица измерения: Метрическая тонна')

    data = pd.read_excel(file_name, usecols='B:F,O', skiprows=int(row[0])+2)

    data.rename(columns={'Код\nИнструмента': 'exchange_product_id',
                         'Наименование\nИнструмента': 'exchange_product_name',
                         'Базис\nпоставки': 'delivery_basis_name',
                         'Объем\nДоговоров\nв единицах\nизмерения': 'volume',
                         'Обьем\nДоговоров,\nруб.': 'total',
                         'Количество\nДоговоров,\nшт.': 'count'
                         }, inplace=True)

    data = data.dropna()
    data.insert(2, 'oil_id', data['exchange_product_id'].str[:4])
    data.insert(3, 'delivery_basis_id', data['exchange_product_id'].str[4:7])
    data.insert(5, 'delivery_type_id', data['exchange_product_id'].str[-1:])
    data['date'] = date
    clean_data = data[data['count'] != '-']

    return clean_data


def delete_file(file):
    """Delete file from server."""
    os.remove(file)


def file_processing(url):
    """Download, process, insert data."""
    name, date = download_file(url)
    data_to_store = retrieve_requested_data_from_file(name, date)
    delete_file(name)
    insert_data_pull_to_db(data_to_store)


with ThreadPoolExecutor() as executor:
    executor.map(file_processing, links)
