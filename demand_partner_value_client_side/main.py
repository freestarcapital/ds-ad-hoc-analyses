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

def main_pageview(force_recalc=False):
    bins = 200

    df = get_bq_data(open(os.path.join(sys.path[0], "pageview_analysis.sql"), "r").read())
    df = df.set_index('pageview_id')
    df2 = df[(df['count_auction_end_raw'] < 10) & (df['count_bidsresponse_raw'] < 50)]

    df3 = df[(df['count_auction_end_raw'] < 10) & (df['count_bidsresponse_raw'] < 50) & (df['count_bidsresponse_raw'] > 0)]

    fig = px.density_heatmap(df2, x='count_auction_end_raw', y='count_bidsresponse_raw', nbinsx=10, nbinsy=50,
                             text_auto=True)
    fig.write_image("pageview_analysis1.png")

    fig = px.density_heatmap(df3, x='count_auction_end_raw', y='count_bidsresponse_raw', nbinsx=10, nbinsy=50,
                             text_auto=True)
    fig.write_image("pageview_analysis2.png")

    f = 0


def main_bidsresponse_bidswon_analysis(force_recalc=False):

    df = get_bq_data(open(os.path.join(sys.path[0], "bidsresponse_bidswon_analysis.sql"), "r").read())
    df = df.set_index('auction_id')
    df2 = df[(df['count_bidswon_raw'] <= 2) & (df['count_bidsresponse_raw'] <= 10)]

    fig = px.density_heatmap(df2, x='count_bidswon_raw', y='count_bidsresponse_raw', nbinsx=3, nbinsy=11,
                             text_auto=True)

    fig.write_image("plots/bidsresponse_bidswon_analysis.png")


if __name__ == "__main__":


#    main_pageview()

    main_bidsresponse_bidswon_analysis()
