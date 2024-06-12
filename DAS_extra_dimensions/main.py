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

def bq_do_query(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace(f"<{k}>", v)
    result = client.query(query).result()
    df = result.to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')
    return df

def main():
    rep_dict = {}

    for (cc_name, cc_query) in [("CC_MERGE", "CASE WHEN session_count < 100 THEN 'default' ELSE country_code END"),
                                ("CC_NO_MERGE", "country_code")]:
        rep_dict['COUNTRY_CODE_NAME'] = cc_name
        rep_dict['COUNTRY_CODE_QUERY'] = cc_query

        for extra_dim in ['ad_product', 'domain']:
            rep_dict['EXTRA_DIM'] = extra_dim

            for fs_testgroup in ['optimised', 'experiment']:
                rep_dict['FS_TESTGROUP'] = fs_testgroup

                query = open(os.path.join(sys.path[0], "create_base_data_query.sql"), "r").read()
                print(f'doing: {cc_name}, {extra_dim}, {fs_testgroup}')
                bq_do_query(query, rep_dict)

            query = open(os.path.join(sys.path[0], "process_results.sql"), "r").read()
            print(f'doing: {cc_name}, {extra_dim}, process results')
            x = bq_do_query(query, rep_dict)
            print(x)
            f = 9

if __name__ == "__main__":
    main()
