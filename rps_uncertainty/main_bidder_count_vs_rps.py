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

def main():

    query = open(os.path.join(sys.path[0], 'query_bidder_count_vs_rps.sql'), "r").read()
    df = get_bq_data(query)
    df = df.set_index('bidders')
    fig, ax = plt.subplots(figsize=(12, 9))
    df.plot(style='x-', ylabel='rps', title='mask bidder count vs average rps', ax=ax)
    fig.savefig('plots/bidder_count_vs_rps.png')

    a=0

if __name__ == "__main__":
    main()