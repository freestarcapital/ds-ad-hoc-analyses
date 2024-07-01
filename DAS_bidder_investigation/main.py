
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime
import pickle
import plotly.express as px
import kaleido
from scipy import stats

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

project_id = "freestar-157323"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{"+k+"}", f'{v}')
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main_ad_requests_by_status():

    bidder = 'rise'
    device_category = 'smartphone-ios'

    # query = open(os.path.join(sys.path[0], "query_rtt_with_numbers.sql"), "r").read()
    # get_bq_data(query, repl_dict)

    repl_dict = {'day_interval': 8,
                 'perc': 0.01,
                 'fallback_rps_perc': 10}

    dt = datetime.date(2024, 6, 27)
    df_list = []
    for d in range(20):
        repl_dict['processing_date'] = (dt - datetime.timedelta(days=d)).strftime("%Y-%m-%d")
        print(f'processing date: {repl_dict['processing_date']}')

        # query = open(os.path.join(sys.path[0], "query_rtt_with_numbers_with_query_rtt_category_raw.sql"), "r").read()
        # get_bq_data(query, repl_dict)

        query = open(os.path.join(sys.path[0], "query_sessions.sql"), "r").read()
        df_list.append(get_bq_data(query, repl_dict))

    df = pd.concat(df_list)
    df = df[(df['bidder'] == bidder) & (df['device_category'] == device_category)]
    df_pivot = df.pivot(columns='status', values='ad_requests_prop', index='date')
    fig, ax = plt.subplots(figsize=(12, 9))
    df_pivot.plot(ylabel='ad_requests proportion', title=f'ad_requests proportion by status for {bidder} {device_category}', ax=ax)
    fig.savefig(f'plots/ad_requests_by_status_{bidder}_{device_category}')

def main_bidder_ordering():
    bidder = 'rise'
    device_category = 'smartphone-ios'
    country_code = 'US'
    status = 'client'

    repl_dict = {'day_interval': 2,
                 'perc': 0.01,
                 'fallback_rps_perc': 10}

    dt = datetime.date(2024, 6, 27)
#    dt = datetime.date(2024, 6, 15)
    df_list = []
    for d in range(6):
        repl_dict['processing_date'] = (dt - datetime.timedelta(days=d)).strftime("%Y-%m-%d")
        print(f'processing date: {repl_dict['processing_date']}')

        # query = open(os.path.join(sys.path[0], "query_rtt_category_raw.sql"), "r").read()
        # get_bq_data(query, repl_dict)

        query = open(os.path.join(sys.path[0], "query_rtt_with_numbers.sql"), "r").read()
        get_bq_data(query, repl_dict)

        query = open(os.path.join(sys.path[0], "bidder_avg_rps.sql"), "r").read()
        x = get_bq_data(query, repl_dict)
        z = x[(x['bidder'] == bidder) & (x['device_category'] == device_category) & (x['country_code'] == country_code) & (x['status'] == status)]
        df_list.append(z)

    df = pd.concat(df_list)
    df_pivot = df.pivot(index='date', columns='rtt_v3', values='status_rank').sort_index()
    fig, ax = plt.subplots(figsize=(12, 9))
    df_pivot.plot(ylabel=f'bidder rank', ax=ax,
                  title=f'bidder rank for status: {status}, bidder: {bidder}, country_code: {country_code}, device_category: {device_category}, day_interval: {repl_dict["day_interval"]}')
    fig.savefig(f'plots/bidder_rank_{status}_{bidder}_{country_code}_{device_category}_dayint_{repl_dict["day_interval"]}')


if __name__ == "__main__":
#    main_ad_requests_by_status()

    main_bidder_ordering()