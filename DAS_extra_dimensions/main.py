import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime
import pickle
import plotly.express as px
import kaleido

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
        query = query.replace(f"<{k}>", v)
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main():
    rep_dict = {"DAYS_BACK_START": "9",
                "DAYS_BACK_END": "2"}

    df_list = []
    for cc in ['_country_merge']:#, '']:

        query = open(os.path.join(sys.path[0], f"base_query{cc}.sql"), "r").read()
        print(f'doing: {cc}, base query')
        df = get_bq_data(query, rep_dict)
        df['cc'] = cc
        df['extra_dim'] = 'None'
        print(df)
        df_list.append(df)

        for extra_dim in ['ad_product']:#, 'domain']:
            rep_dict['EXTRA_DIM'] = extra_dim

            query = open(os.path.join(sys.path[0], f"extra_dims_query{cc}.sql"), "r").read()
            print(f'doing: {cc}, {extra_dim}')
            df = get_bq_data(query, rep_dict)
            df['cc'] = cc
            df['extra_dim'] = extra_dim
            print(df)
            df_list.append(df)

    df_results = pd.concat(df_list)
    df_results.to_csv(f'results_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}.csv')
    print(df_results)

    h = 0

if __name__ == "__main__":
    main()
