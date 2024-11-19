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

def get_data(query_filename, data_cache_filename, force_requery=False, repl_dict = {}):
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


def main_pageview(force_requery=False):
    max_duration = 60 * 60 # equals one hour

    df_raw = get_data('pageview_duration', 'pageview_duration', force_requery)

    duration = df_raw['duration_ms'].sort_values(inplace=False).values / 1000
    duration[duration > max_duration] = max_duration
    df = pd.Series(np.arange(len(duration)) / len(duration), index=pd.Index(duration))

    df_ = df.iloc[np.arange(0, len(df), 100)]
    j = 0

    df_5 = df[df.index <= 5]
    fig, ax = plt.subplots(figsize=(12, 9))
    df_5.plot(ax=ax, xlabel='pageview duration in seconds', title='Cumulative proportion of page views that last up to 5 secs', ylabel='proportion')
    fig.savefig('plots/pageview_duration.png')


def main_response_brr(force_requery=False):
    max_duration = 60 * 60 # equals one hour

    df_raw = get_data('pageview_duration_using_brr', 'pageview_duration_using_brr', force_requery)
    # in_table_prop = df_raw['is_in_asr_table'].mean()
    # expected_zeros = 1 - 0.8 * in_table_prop
    expected_zeros = 0.1

    for col in ['hit_to_pv_servertime', 'hit_to_max_time_brr']:

        duration = df_raw[col].sort_values(inplace=False).values / 1000
        duration[duration > max_duration] = max_duration

        duration = duration[duration > 0]
        x_0_1 = np.arange(len(duration)) / len(duration)
        x_expected_zeros_1 = expected_zeros + (1 - expected_zeros) * x_0_1
        df = pd.Series(x_expected_zeros_1, index=pd.Index(duration))

        # duration[duration < 0] = 0
        # x_0_1 = np.arange(len(duration)) / len(duration)
        # x_expected_zeros_1 = expected_zeros + (1 - expected_zeros) * x_0_1
        # df = pd.Series(x_expected_zeros_1, index=pd.Index(duration))

        df_5 = df[df.index <= 5]
        fig, ax = plt.subplots(figsize=(12, 9))
        df_5.plot(ax=ax, xlabel=f'{col} in seconds', title=f'Cumulative proportion of {col}', ylabel='proportion')
        fig.savefig(f'plots/duration_{col}.png')


if __name__ == "__main__":
    #main_pageview()

    main_response_brr()
