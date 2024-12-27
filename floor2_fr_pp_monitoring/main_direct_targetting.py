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
        query = query.replace("{" + k + "}", f'{v}')

    result = client.query(query).result()
    if result is None:
        return

    return result.to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

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

def main_base():

    repl_dict = {'first_date': '2024-11-1',
                 'last_date': '2024-12-10'}

    query_file = 'query_fill_rate_base'
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    get_bq_data(query, repl_dict)

def main():

    repl_dict = {'ad_unit_name': 'ad_unit_name = "/15184186/signupgenius_Desktop_SignUps_Sheet_300x600_Right"'}

    query_file = 'query_direct_targetting'
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    df = get_bq_data(query, repl_dict)

    plot_specs = [(['optimised_requests'], True), (['optimised_fill_rate'], False), (['optimised_cpm', 'optimised_cpma'], False)]

    fig, ax = plt.subplots(figsize=(16, 12), nrows=len(plot_specs))

    for i, (cols, log_y) in enumerate(plot_specs):
        ax_i = ax[i]

        df[cols].plot(ax=ax_i, logy=log_y)

    fig.savefig(f'plots_direct/plot.png')

if __name__ == "__main__":
    #main_base()
    main()