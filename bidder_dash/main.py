import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime
import pickle
from matplotlib.backends.backend_pdf import PdfPages

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

project_id = "streamamp-qa-239417"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{"+k+"}", f'{v}')
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')


def get_data_using_query(query, filename, index=None, force_calc=False, repl_dict={}):
    data_cache_filename = f'data_cache/wb_{filename}.pkl'
    if not force_calc and os.path.exists(data_cache_filename):
        print(f'found existing data file, loading: {data_cache_filename}')
    else:
        print(f'{datetime.datetime.now()}: querying to create file: {data_cache_filename}')
        df = get_bq_data(query, repl_dict)
        if index is not None:
            df = df.set_index(index)

        with open(data_cache_filename, 'wb') as f:
            pickle.dump(df, f)

    with open(data_cache_filename, 'rb') as f:
        df = pickle.load(f)
    return df


def main_create_bidder_session_data_raw(last_date=datetime.date(2024, 9, 1), days=30):

    repl_dict = {'project_id': project_id,
                 'processing_date': last_date,
                 'days_back_start': days,
                 'days_back_end': 1,
                # 'aer_to_bwr_join_type': ' left join '
                 'aer_to_bwr_join_type': ' join '}

    query = open(os.path.join(sys.path[0], 'queries/query_bidder_session_data_raw.sql'), "r").read()
    get_bq_data(query, repl_dict)

    # query = open(os.path.join(sys.path[0], 'queries/query_bidder_session_data_raw_domain.sql'), "r").read()
    # get_bq_data(query, repl_dict)

def main_country_code():

    repl_dict = {
        'project_id': project_id,
        'dimensions': 'date, bidder, country_code',
        'tablename': 'bidder_session_data_raw_2024-08-20_30_1',
        'where': f"where status = 'client' and bidder not in ('amazon', 'preGAMAuction') and device_category='desktop'"
    }

    query = open(os.path.join(sys.path[0], 'queries/query_get_rps_and_uncertainty.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df['date'] = pd.to_datetime(df['date'])

    country_codes = df[['country_code', 'session_count']].groupby('country_code').sum().sort_values('session_count',
                                                                                                   ascending=False).index[:30]

    with PdfPages(f'plots/rps_country_code.pdf') as pdf:
        for cc in country_codes:

            df_cc = df[df['country_code'] == cc]

            df_p_dict = {}
            for value in ['session_count', 'rps', 'rps_std']:
                df_p = df_cc.pivot(index='date', columns='bidder', values=value).fillna(0)
                df_p = df_p.iloc[1:, :]
                df_p = df_p[df_p.iloc[-1, :].sort_values(ascending=False).index]
                df_p_dict[value] = df_p

            fig, ax = plt.subplots(figsize=(16, 12), ncols=2)
            fig.suptitle(f'{cc}, desktop')
            df_p_dict['rps'].plot(yerr=df_p_dict['rps_std'], ax=ax[0], ylabel='rps with uncertainty')
            df_p_dict['session_count'].plot(ax=ax[1], logy=True, ylabel='session count')

            fig.savefig(f'plots/country_pngs/rps_{cc}.png')
            pdf.savefig()


if __name__ == "__main__":
    #main_create_bidder_session_data_raw()
    main_country_code()
