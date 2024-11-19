import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime
import pickle
import scipy
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


def main():
    query = open(os.path.join(sys.path[0], f"queries/raw_dtf_session_data.sql"), "r").read()
    get_bq_data(query)

def main_iai_performance():

    query = open(os.path.join(sys.path[0], f"queries/iai_performance_stats.sql"), "r").read()
    df_all = get_bq_data(query)

    fig, ax = plt.subplots(figsize=(12, 9), nrows=3, ncols=2)
    ax = ax.flatten()

    for i, col in enumerate(['rps_iai', 'rps_total', 'impressions_iai', 'impressions_total', 'unfilled_iai', 'unfilled_total']):
        ax_i = ax[i]

        df = df_all.pivot(index='date', columns='percentile_placement', values=col)
        df = df['2024-11-12':'2024-11-16']
        pp_mean = df.mean()
        pp_mean_uncertainty = df.std() / np.sqrt(len(df))

        results_list = []
        for i1 in range(len(pp_mean)):
            for i2 in range(len(pp_mean)):
                z_score = (pp_mean.iloc[i1] - pp_mean.iloc[i2]) / np.sqrt(
                    pp_mean_uncertainty.iloc[i1] ** 2 + pp_mean_uncertainty.iloc[i2] ** 2)
                p_value = scipy.stats.norm.sf(abs(z_score))
                if np.sign(z_score) == -1:
                    p_value = -p_value

                results_list.append({'i1': pp_mean.index[i1],
                                     'i2': pp_mean.index[i2],
                                     'z_score': z_score,
                                     'p_value': p_value})

        p_value_table = pd.DataFrame(results_list).pivot(index='i1', columns='i2', values='p_value') * 100
        df.plot(title=col, ylabel=col, ax=ax_i)

    fig.savefig(f'plots/iai_performance.png')


if __name__ == "__main__":

    main()
#    main_iai_performance()
