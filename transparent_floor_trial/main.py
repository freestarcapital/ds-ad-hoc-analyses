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


def get_domains_from_collection_ids(collection_ids, start_date=dt.date.today()-dt.timedelta(days=21), end_date=dt.date.today()):
    min_page_hits = 10000

    print(f'searching for domains for collection_ids: {", ".join(collection_ids)} from {start_date} to {end_date}, min_page_hits: {min_page_hits}')

    repl_dict = {'start_date': start_date.strftime("%Y-%m-%d"),
                 'end_date': end_date.strftime("%Y-%m-%d"),
                 'collection_ids_list': f"('{"', '".join(collection_ids)}')",
                 'min_page_hits': min_page_hits}

    query = open(os.path.join(sys.path[0], "queries/query_get_domains_from_collection_ids.sql"), "r").read()
    df = get_bq_data(query, repl_dict)
    return df['domain'].to_list()


def get_domains_from_test_names(test_names, start_date=dt.date.today()-dt.timedelta(days=21), end_date=dt.date.today()):

    print(f'searching for domains for test_names: {", ".join(test_names)} from {start_date} to {end_date}')

    repl_dict = {'start_date': start_date.strftime("%Y-%m-%d"),
                 'end_date': end_date.strftime("%Y-%m-%d"),
                 'test_names_list': f"('{"', '".join(test_names)}')"}

    query = open(os.path.join(sys.path[0], "queries/query_get_domains_from_test_names.sql"), "r").read()
    df = get_bq_data(query, repl_dict)
    return df['domain'].unique().to_list()


def main_dash():

    #query_filename = 'query_transparent_floors'
    #tablename = f"streamamp-qa-239417.DAS_increment.transparent_bidder_participation"

    query_filename = 'query_BI_AB_test_original'
    #query_filename = 'query_BI_AB_test_page_hits'

    datelist = pd.date_range(start=dt.date(2025,8,26), end=dt.date(2025,9,1))
    name = 'timeouts'
    test_domains = get_domains_from_collection_ids(['9c42ef7c-2115-4da9-8a22-bd9c36cdb8b4'])

    # datelist = pd.date_range(end=dt.datetime.today().date(), periods=32)
    # name = 'transparent_floors'
    # test_domains = [
    #     'pro-football-reference.com',
    #     'baseball-reference.com',
    #     'deepai.org',
    #     'signupgenius.com',
    #     'perchance.org'
    # ]

    tablename = f"streamamp-qa-239417.DAS_increment.{query_filename.replace('query_', '')}_results_{name}"
    first_row = True

    domain_list = f"('{"', '".join(test_domains)}')"
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    print(f'query_filename: {query_filename} for domain_list: {domain_list}')

    for date in datelist.tolist():
        create_or_insert_statement = f"CREATE OR REPLACE TABLE `{tablename}` as" if first_row else f"insert into `{tablename}`"
        first_row = False
        print(f'date: {date}: {create_or_insert_statement}')

        repl_dict = {'ddate': date.strftime("%Y-%m-%d"),
                     'create_or_insert_statement': create_or_insert_statement,
                     'domain_list': domain_list,
                     'name': name}
        get_bq_data(query, repl_dict)


if __name__ == "__main__":

    main_dash()
