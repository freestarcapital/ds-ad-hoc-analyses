import numpy as np
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
from scipy import stats

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

def get_data(last_date, days, force_recalc=False):

    repl_dict = {'project_id': project_id,
                 'processing_date': last_date,
                 'days_back_start': days,
                 'days_back_end': 1}

    if force_recalc:
        query = open(os.path.join(sys.path[0], "query_eventstream_DAS_expt_stats.sql"), "r").read()
        get_bq_data(query, repl_dict)

    return f'eventstream_DAS_expt_stats_{repl_dict["processing_date"]}_{repl_dict["days_back_start"]}_{repl_dict["days_back_end"]}'


def main(last_date, days, do_log=False, force_recalc=False):

    eventstream_session_data_tablename = get_data(last_date, days, force_recalc)

    number_of_buckets = 1000

    repl_dict = {'number_of_buckets': number_of_buckets,
                 'bidder_mask': '....2..................',
                 'eventstream_session_data_tablename':  eventstream_session_data_tablename}

    df_list = []
    for samples_per_bucket in [20, 100, 500, 2500, 12500]:

        repl_dict['samples_per_bucket'] = samples_per_bucket
        print(f'doing {repl_dict['samples_per_bucket']}')
        query = open(os.path.join(sys.path[0], "query_get_bidder_status_rps.sql"), "r").read()
        df = get_bq_data(query, repl_dict)
        df_list.append(df[['bucket_rps']].rename(columns={'bucket_rps': samples_per_bucket}))

    df = pd.concat(df_list, axis=1)

    do_plots(df, f'plots/bucket_rps_linear.png')

    do_plots(np.log(df), f'plots/bucket_rps_log.png')

def do_plots(df, filename):
    df_mean = df.mean()
    df_std = df.std()

    col_names = [f'{c} sessions, mean: {df_mean[c]:0.2f}, std: {df_std[c]:0.2f}, std * sqrt({c}): {df_std[c] * np.sqrt(c):0.2f}' for c in df.columns]
    df_max = 15
    df[df > df_max] = df_max
    y_pdf, x_pdf, _ = plt.hist(df, 100, density=True)
    df_hist_pdf = pd.DataFrame(y_pdf.transpose(), index=pd.Index(x_pdf[:-1]), columns=col_names)
    y_cdf, x_cdf, _ = plt.hist(df, 100, density=True, cumulative=True)
    df_hist_cdf = pd.DataFrame(y_cdf.transpose(), index=pd.Index(x_cdf[:-1]), columns=col_names)
    fig, ax = plt.subplots(figsize=(12, 9), nrows=2)
    df_hist_pdf.plot(ax=ax[0], title='Histogram of rps measured over different session lengths (shown in legend)', xlabel='rps', ylabel='pdf')
    df_hist_cdf.plot(ax=ax[1], xlabel='rps', ylabel='cdf')

    fig.savefig(filename)

    h = 0


if __name__ == "__main__":

    main(last_date=datetime.date(2024, 8, 20), days=30, force_recalc=False)
    main(last_date=datetime.date(2024, 8, 20), days=30, do_log=True, force_recalc=False)