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
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')
def main():
    for rolling_days in [1, 7, 14]:

        rep_dict = {'N_days': rolling_days}

        query = open(os.path.join(sys.path[0], f"query_performance_eval_3.sql"), "r").read()
        df = get_bq_data(query, rep_dict)

        df = df.set_index('date').sort_index()
        df = df.iloc[rolling_days:]
        fig, ax = plt.subplots(figsize=(12, 9))
        ((df['rps_rolling_change'] / df['rps_rolling_control'] - 1) * 100).plot(ax=ax,
            title=f'optimised vs experiment uplift for domain override cohorts, using rolling average revenue and session_count with {rolling_days} day lookback')
#        df.plot(ax=ax, title=f'change cohorts vs no change cohorts rps uplift, using rolling average revenue and session_count with {rolling_days} day lookback')
        fig.savefig(f'plots/rps_uplift_domain_rolling_{rolling_days}.png')


if __name__ == "__main__":
    main()

