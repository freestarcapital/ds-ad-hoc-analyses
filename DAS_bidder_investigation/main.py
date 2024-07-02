
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

def get_data(last_date=datetime.date.today() - datetime.timedelta(days=1), days=30, force_recalc=False):

    data_cache_filename = f'data_cache/DAS_bidder_investigation_{last_date}_{days}.pkl'

    if not force_recalc and os.path.exists(data_cache_filename):
        print(f'found existing data file, loading {data_cache_filename}')
        with open(data_cache_filename, 'rb') as f:
            df = pickle.load(f)
        return df

    repl_dict = {'day_interval': 2,
                 'perc': 0.01,
                 'fallback_rps_perc': 10}

    df_list = []
    for d in range(days):
        repl_dict['processing_date'] = (last_date - datetime.timedelta(days=d)).strftime("%Y-%m-%d")
        print(f'processing date: {repl_dict['processing_date']}')

        query = open(os.path.join(sys.path[0], "query_rtt_with_numbers.sql"), "r").read()
        get_bq_data(query, repl_dict)

        query = open(os.path.join(sys.path[0], "bidder_avg_rps.sql"), "r").read()
        df_day = get_bq_data(query, repl_dict)
        df_list.append(df_day)

    df = pd.concat(df_list)
    with open(data_cache_filename, 'wb') as f:
        pickle.dump(df, f)
    return df

def main(last_date=datetime.date.today() - datetime.timedelta(days=1), days=30, force_recalc=False):
    client_rank_limit = 8
    client_or_server_rank_limit = 13
    df = get_data(last_date, days, force_recalc)
    df = df.set_index(pd.to_datetime(df['date']))
    df = df.sort_index()

    bidder = 'rise'
    device_category = 'smartphone-ios'
    country_code = 'US'

    df = df[(df['bidder'] == bidder) & (df['device_category'] == device_category) & (df['country_code'] == country_code)]
    df[f'is_client_top_{client_rank_limit}'] = df['client_rank'] <= client_rank_limit
    df[f'is_client_or_server_top_{client_or_server_rank_limit}'] = df['client_or_server_rank'] <= client_or_server_rank_limit

    fig, ax = plt.subplots(figsize=(16, 12), nrows=3, ncols=3)
    for i, rtt in enumerate(np.sort(np.array(df['rtt_v3'].unique()))):

        df_rtt = df[df['rtt_v3'] == rtt]

        df_rtt[['client_rank', 'client_bidders', 'client_or_server_rank', 'client_or_server_bidders']].plot(style='x-',
                ax=ax[i, 0], ylabel=f'rtt: {rtt}')

        (1 * df_rtt[['is_client', f'is_client_top_{client_rank_limit}']]).plot(style='x-',
                ax=ax[i, 1], ylabel=f'rtt: {rtt}')

        (1 * df_rtt[['is_client_or_server', f'is_client_or_server_top_{client_or_server_rank_limit}']]).plot(style='x-',
                ax=ax[i, 2], ylabel=f'rtt: {rtt}')

    fig.suptitle(f'Bidder Report for bidder: {bidder} in segment: {country_code}, {device_category}')
    fig.savefig(f'plots/bidder_investigation_{bidder}_{country_code}_{device_category}_{last_date}_{days}.png')


if __name__ == "__main__":

    main(last_date=datetime.date(2024, 6, 30), days=90)