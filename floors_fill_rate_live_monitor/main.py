import dateutil.utils
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


    query = open(os.path.join(sys.path[0], f"query_get_live_data.sql"), "r").read()
    df = get_bq_data(query)

    df_p = df.pivot(index='date_hour', columns='placement_id', values='avg_floor_price')

    fig, ax = plt.subplots(figsize=(12, 9))
    df_p.plot(ax=ax)
    fig.savefig(f'plot.png')


if __name__ == "__main__":
    main()

