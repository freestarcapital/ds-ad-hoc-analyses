import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime
import pickle
import scipy
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.linear_model import LinearRegression

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

project_id = "streamamp-qa-239417"
client = bigquery.Client(project=project_id)#, location='US')
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{"+k+"}", f'{v}')

    result = client.query(query).result()
    if result is None:
        return

    return result.to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def get_data(query_filename, data_cache_filename, force_requery=False, repl_dict=None):
    if repl_dict is None:
        repl_dict = {}
    data_cache_filename_full = f'data_cache/{data_cache_filename}.pkl'

    if not force_requery and os.path.exists(data_cache_filename_full):
        print(f'found existing data file, loading {data_cache_filename_full}')
        with open(data_cache_filename_full, 'rb') as f:
            df = pickle.load(f)
        return df

    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    df = get_bq_data(query, repl_dict)

    with open(data_cache_filename_full, 'wb') as f:
        pickle.dump(df, f)
    return df


def main():
    ad_unit_name = '/15184186/signupgenius_Mobile_Anchor_320x50'

    query_file = 'query_fill_rate_tracking'
    repl_dict = {'ad_unit_name': ad_unit_name}
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    df_all = get_bq_data(query, repl_dict).set_index('date_hour')

    cols = ['floor_price', 'optimised_fill_rate'] # , 'optimised_cpma']

    stats_dict = {}
    fig, ax = plt.subplots(figsize=(12, 9), nrows=len(cols))
    for i, c in enumerate(cols):
        df = df_all[c]

        fig.suptitle(ad_unit_name)
        ax_i = ax[i]
        df.plot(ylabel=c, ax=ax_i)

        stats_dict[f'mean_{c}'] = df.mean()
        stats_dict[f'std_{c}'] = df.std()

    fig.savefig(f'plots_fill_rate_tracking/plot_{ad_unit_name.replace('/','_')}.png')


    stats_hour_list = []
    for i, c in enumerate(cols):
        fig, ax = plt.subplots(figsize=(16, 12))
        fig.suptitle(f'{ad_unit_name}, {c}')

        df = df_all.pivot(index='date', columns='hour', values=c)
        df.plot(ax=ax)
        fig.savefig(f'plots_fill_rate_tracking/plot_hours_{ad_unit_name.replace('/', '_')}_{c}.png')

        stats = df.mean().to_frame(f'mean_{c}')
        stats[f'std_{c}'] = df.std()
        stats_hour_list.append(stats)

    df_stats = pd.concat(stats_hour_list, axis=1)
    df_stats = pd.concat([pd.DataFrame(stats_dict, index=pd.Index(['all_hours'])), df_stats], axis=0)
    df_stats.to_csv(f'plots_fill_rate_tracking/stats_{ad_unit_name.replace('/', '_')}.csv')

    with PdfPages(f'plots_fill_rate_tracking/scatter_plots_{ad_unit_name.replace('/', '_')}.pdf') as pdf:
        R_sq = {}
        for c in ['optimised_fill_rate', 'price_pressure']:
            reg = LinearRegression().fit(df_all[['floor_price']], df_all[c])
            r2 = reg.score(df_all[['floor_price']], df_all[c])
            R_sq[f'all_hours_{c}'] = r2
            fig, ax = plt.subplots(figsize=(12, 9))
            fig.suptitle(f'all_hours_{c}, {c} ~ {reg.intercept_:0.2f} + {reg.coef_[0]:0.3f} x floor_price, R^2: {r2*100:0.1f}%')
            df_all.plot.scatter('floor_price', c, ax=ax)
            pdf.savefig()
            fig.savefig(f'plots_fill_rate_tracking/scatter_all_hours_{ad_unit_name.replace('/', '_')}_{c}.png')

            for h in range(24):
                df_h = df_all[df_all['hour'] == h]
                reg = LinearRegression().fit(df_h[['floor_price']], df_h[c])
                r2 = reg.score(df_h[['floor_price']], df_h[c])
                R_sq[f'hour_{h}_{c}'] = r2
                fig, ax = plt.subplots(figsize=(12, 9))
                fig.suptitle(f'hour_{h}_{c}, {c} ~ {reg.intercept_:0.2f} + {reg.coef_[0]:0.3f} x floor_price, R^2: {r2 * 100:0.1f}%')
                df_h.plot.scatter('floor_price', c, ax=ax)
                pdf.savefig()
    f = 0

if __name__ == "__main__":

    main()
