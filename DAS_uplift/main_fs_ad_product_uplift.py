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

    configs = [
                    ['country_code'],
                    ['country_code', 'device_category'],
                    ['country_code', 'device_category', 'rtt_category'],
                    ['country_code', 'domain'],
                    ['country_code', 'domain', 'device_category']]#,
                    #['country_code', 'domain', 'device_category', 'rtt_category']]

    results_list = []
    for c in configs:
        dims = ', '.join(c)
        print(f'doing dims: {dims}')

        tablename_dims = '_'.join([c_[:3] for c_ in c])
        tablename_dims_ad = tablename_dims + '_ad'
        if 'rtt' in dims:
            tablename_dims_ad = tablename_dims.replace('rtt', 'ad_rtt')

        repl_dict = {'dims': dims,
                     'tablename_dims': tablename_dims,
                     'tablename_dims_ad': tablename_dims_ad}

        query = open(os.path.join(sys.path[0], f"query_fs_ad_product_uplift.sql"), "r").read()
        df = get_bq_data(query, repl_dict)
        df['dims'] = dims
        results_list.append(df)

    results_df = pd.concat(results_list)

    results_df['ad_prop'] = results_df['sessions_ad'] / (results_df['sessions_ad'] + results_df['sessions_no_ad'])
    results_df['total_uplift'] = results_df['ad_prop'] * results_df['rps_uplift_ad_weighted']
    total_uplift = (results_df['total_uplift'] * results_df['sessions_ad']).sum() / results_df['sessions_ad'].sum()

    results_df.to_csv('fs_ad_product_uplift.csv')

    f = 0

if __name__ == "__main__":
    main()

