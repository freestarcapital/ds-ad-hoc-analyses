import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime
import pickle
import plotly.express as px
import kaleido
from scipy.stats import linregress

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
        query = query.replace(f"<{k}>", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')


def get_data(bidder, days_back_start, days_back_end, force_recalc):
    data_cache_filename = f'data_cache/DAS_new_bidder_{bidder}_{days_back_start}_{days_back_end}.pkl'

    if not force_recalc and os.path.exists(data_cache_filename):
        print(f'found existing data file, loading {data_cache_filename}')
        with open(data_cache_filename, 'rb') as f:
            df = pickle.load(f)
        return df

    repl_dict = {"DAYS_BACK_START": days_back_start,
                "DAYS_BACK_END": days_back_end,
                 "BIDDER": bidder}
    query = open(os.path.join(sys.path[0], "bidder_query.sql"), "r").read()
    df = get_bq_data(query, repl_dict)

    with open(data_cache_filename, 'wb') as f:
        pickle.dump(df, f)
    return df

def main(bidder="insticator", days_back_start=32, days_back_end=2, force_recalc=False):

    df = get_data(bidder, days_back_start, days_back_end, force_recalc)

    df_bidder = df[df['bidder'] == bidder]
    df_bidder.to_csv(f'results/new_bidder_{bidder}_{days_back_start}_{days_back_end}.csv')

    df_comparison = df[['bidder', 'makes_cut']].groupby('bidder').sum().sort_values(by='makes_cut', ascending=False)
    df_comparison.to_csv(f'results/new_bidder_comparison_{bidder}_{days_back_start}_{days_back_end}.csv')

if __name__ == "__main__":

    main()

