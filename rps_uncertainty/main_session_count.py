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

    for f in ['session_count', 'session_count_refresh']:
        query = open(os.path.join(sys.path[0], f'query_{f}.sql'), "r").read()
        df = get_bq_data(query)
        df.to_csv(f'plots/{f}.csv')


    h=0

def main_N():
    repl_dict = {'mult': 1}

    query = open(os.path.join(sys.path[0], 'query_session_count_N.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df = df.set_index('f0_')
    df = df.loc[df.iloc[:, -1].sort_values().index]

    df.to_csv(f'plots/session_count_N_{repl_dict['mult']}.csv')


if __name__ == "__main__":
    main_N()
#    main()
