import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime as dt
import pickle
import plotly.express as px
import kaleido
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

project_id = "streamamp-qa-239417"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_data(query_filename, data_cache_filename=None, force_requery=False, repl_dict={}):

    if data_cache_filename is None:
        data_cache_filename = query_filename
    data_cache_filename_full = f'data_cache/{data_cache_filename}.pkl'

    if not force_requery and os.path.exists(data_cache_filename_full):
        print(f'found existing data file, loading {data_cache_filename_full}')
        with open(data_cache_filename_full, 'rb') as f:
            df = pickle.load(f)
        return df

    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    df = get_bq_data(query, repl_dict)

    with open(data_cache_filename_full, 'wb') as f:
        pickle.dump(df, f)
    return df

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main_dash():
    test_domains = [
        'pro-football-reference.com',
        'baseball-reference.com',
        'deepai.org',
        'signupgenius.com'
    ]

    tablename = f"streamamp-qa-239417.DAS_increment.transparent_bidder_participation"

    datelist = pd.date_range(end=dt.datetime.today().date(), periods=28)
    #datelist = pd.date_range(end=dt.date(2025, 8, 15), periods=2)
    first_row = True

    query_filename = f"queries/query_transparent_floors.sql"
    query = open(os.path.join(sys.path[0], query_filename), "r").read()
    print(f'query_filename: {query_filename}')

    domain_list = f"({', '.join([f"'{d}'" for d in test_domains])})"

    for date in datelist.tolist():
        print(f'date: {date}')

        create_or_insert_statement = f"CREATE OR REPLACE TABLE `{tablename}` as" if first_row else f"insert into `{tablename}`"
        first_row = False

        repl_dict = {'ddate': date.strftime("%Y-%m-%d"),
                     'create_or_insert_statement': create_or_insert_statement,
                     'domain_list': domain_list}
        get_bq_data(query, repl_dict)

if __name__ == "__main__":

    main_dash()
