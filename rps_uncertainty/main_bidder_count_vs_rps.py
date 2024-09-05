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

#    repl_dict = {'table_ext': '2024-08-20_30_1'}
    repl_dict = {'table_ext': '2024-09-05_7_1',
                 'DTF_or_eventstream': 'eventstream'}
                # 'DTF_or_eventstream': 'DTF'}

    query = open(os.path.join(sys.path[0], 'query_bidder_count_vs_rps.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df = df.set_index('bidders')
    df = df.iloc[:, :6]

    fig, ax = plt.subplots(figsize=(12, 9))
    df_avg_cols = [c for c in df.columns if '_err_' not in c]
    df_avg = df[df_avg_cols]
    df_avg_err_cols = [c for c in df.columns if '_err_' in c]
    df_avg_err = df[df_avg_err_cols]
    df_avg_err = df_avg_err.rename(columns=dict(zip(df_avg_err_cols, df_avg_cols)))

    df_avg.plot(style='x-', ylabel='rps', title='mask bidder count vs average rps', ax=ax, yerr=df_avg_err)
    fig.savefig(f'plots/bidder_count_vs_rps_{repl_dict['DTF_or_eventstream']}_{repl_dict['table_ext']}.png')


def main_country():

#    repl_dict = {'table_ext': '2024-08-20_30_1'}
    repl_dict = {'table_ext': '2024-09-05_7_1',
                 'DTF_or_eventstream': 'eventstream'}
                # 'DTF_or_eventstream': 'DTF'}

#    panels = ['client_bidders', 'server_bidders', 'all_bidders']
    panels = ['client_bidders', 'all_bidders']

    include_counts = False
    N = 10
    query = open(os.path.join(sys.path[0], 'query_bidder_count_vs_rps_country_or_device.sql'), "r").read()

    for cd in ['device_category', 'country_code']:
        repl_dict['country_or_device'] = cd

        if include_counts:
            fig, ax = plt.subplots(figsize=(20, 16), ncols=len(panels), nrows=2)
        else:
            fig, ax = plt.subplots(figsize=(16, 12), ncols=len(panels))

        for i, bidders in enumerate(panels):
            repl_dict['which_bidders'] = bidders
            df = get_bq_data(query, repl_dict)
            df = df[df[bidders] > 0]
            top_N = df[[cd, 'count']].groupby(cd).sum().sort_values('count', ascending=False)[:N].index

            df_p = df.pivot(index=bidders, columns=cd, values='rps')
            df_p = df_p[top_N]
            col_order = df_p.iloc[0].sort_values(ascending=False).index
            df_p = df_p[col_order]

            df_p_err = df.pivot(index=bidders, columns=cd, values='rps_err')
            df_p_err = df_p_err[top_N]
            df_p_err = df_p_err[col_order]

            if include_counts:
                df_p.plot(style='x-', ylabel='rps', ax=ax[0, i], title=bidders, yerr=df_p_err)

                df_p = df.pivot(index=bidders, columns=cd, values='count')
                df_p = df_p[top_N]
                df_p[col_order].plot(style='x-', ylabel='count', ax=ax[1, i], title=bidders, logy=True)
            else:
                df_p.plot(style='x-', ylabel='rps', ax=ax[i], title=bidders, yerr=df_p_err)


        fig.savefig(f'plots/bidder_count_vs_rps_{cd}_{repl_dict['DTF_or_eventstream']}_{repl_dict['table_ext']}.png')

    a=0

if __name__ == "__main__":
    main()
    #main_country()