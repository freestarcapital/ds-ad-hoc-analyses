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

project_id = "streamamp-qa-239417"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()


def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main_create_bidder_domain_expt_session_stats(last_date=datetime.date(2024, 9, 9), days=30):

    repl_dict = {'project_id': project_id,
                 'processing_date': last_date,
                 'days_back_start': days,
                 'days_back_end': 1,
                # 'aer_to_bwr_join_type': 'left join'
                 'aer_to_bwr_join_type': 'join'}

    query = open(os.path.join(sys.path[0], 'queries/query_daily_bidder_domain_expt_session_stats.sql'), "r").read()
    get_bq_data(query, repl_dict)


def main_create_daily_configs():

    processing_date = datetime.date(2024, 8, 20)

    repl_dict = {'project_id': project_id,
                 'tablename_from': 'daily_bidder_domain_expt_stats_join_2024-09-09_30_1',
                 'tablename_to': 'DAS_config',
                 'processing_date': '2024-09-04',
                 'days_back_start': 7,
                 'days_back_end': 1,
                 'min_all_bidder_session_count': 50000,
                 'min_individual_bidder_session_count': 1000}

    query = open(os.path.join(sys.path[0], 'queries/query_create_daily_country_config.sql'), "r").read()
    get_bq_data(query, repl_dict)


if __name__ == "__main__":

#    main_create_bidder_domain_expt_session_stats()
    main_create_daily_configs()

