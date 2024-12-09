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
    repl_dict = {'first_date': '2024-12-5',
                 'ad_unit_name': '/15184186/signupgenius_sticky_footer',
                 'N': 23}

    query = open(os.path.join(sys.path[0], f"queries/query_fill_rate.sql"), "r").read()
    df_all = get_bq_data(query, repl_dict).set_index('date_hour')

    for base_col in ['fill_rate', 'per_day', 'perc']:

        cols = [c for c in df_all.columns if (base_col in c) and ('err' not in c)]
        df = df_all[cols]

        fig, ax = plt.subplots(figsize=(12, 9))

        err_cols = [c for c in df_all.columns if (base_col in c) and ('err' in c)]
        if True:#len(err_cols) == 0:
            df_err = df_all[err_cols].rename(columns=dict([(c, c.replace('_err', '')) for c in err_cols]))
            df.plot(ax=ax, title=base_col, yerr=df_err)
        else:
            df.plot(ax=ax, title=base_col)

        fig.savefig(f'plots/raw_{base_col}.png')


    c = 0

if __name__ == "__main__":

    main()