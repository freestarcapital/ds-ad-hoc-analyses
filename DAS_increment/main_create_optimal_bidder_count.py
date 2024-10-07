import dateutil.utils
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime as dt
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


def main_create_optimial_bidder_count(last_date, days):
    repl_dict = {'project_id': project_id,
                 'tablename_from': f'daily_bidder_domain_expt_session_stats_unexpanded_join_{last_date.strftime("%Y-%m-%d")}_{days}_1',
                 'dims': 'date, country_code',
                 'and_where': ' and country_code = "US" and device_category = "desktop"'}

    query = open(os.path.join(sys.path[0], 'queries/query_create_optimial_bidder_count.sql'), "r").read()
    df = get_bq_data(query, repl_dict)

    df = df[['date', 'bidders', 'rps_client']]
    df = df[~df['rps_client'].isna()]

    fig, ax = plt.subplots(figsize=(12, 9))
    df.pivot(index='date', columns='bidders', values='rps_client')[1:].plot(ax=ax)
    fig.savefig('plots/bidder_count_1.png')

    fig, ax = plt.subplots(figsize=(12, 9))
    df.pivot(index='bidders', columns='date', values='rps_client').iloc[:, 1:].plot(ax=ax)
    fig.savefig('plots/bidder_count_2.png')

    df = df.pivot(index='bidders', columns='date', values='rps_client').iloc[:, 1:]
    df_m = df.mean(axis=1).to_frame('rps')
    df_s = df.std(axis=1).to_frame('rps')

    df_mc = df_m.copy()
    for p in [1.5, 2.5, 5, 10]:
        p_str = f'mult_{p * 10:0.0f}'
        df_mc[p_str] = (1 + p / 100) ** np.arange(len(df_m))
        df_mc[p_str] = df_mc[p_str] / df_mc[p_str][10]
        df_m[f'rps_adj_{p * 10:0.0f}'] = df_m['rps'] / df_mc[p_str]

    fig, ax = plt.subplots(figsize=(12, 9))
    df_m.plot(ax=ax)
    fig.savefig('plots/bidder_count_3.png')

    h = 0


def main_bidder_count(last_date, days):
    repl_dict = {'project_id': project_id,
                 'tablename_from': f'daily_bidder_domain_expt_session_stats_cbc_join_{last_date.strftime("%Y-%m-%d")}_{days}_1',
                 'dims': 'date, country_code',
                 'and_where': ' and country_code = "US" and device_category = "desktop"'}

    query = ('select bidder, client_bidders, '
             'sum(session_count) session_count, sum(revenue) revenue, safe_divide(sum(revenue), sum(session_count)) * 1000 rps '
             f'from `{project_id}.DAS_increment.{repl_dict["tablename_from"]}` '
             f'where status="client" and bidder not in ("preGAMAuction", "amazon", "seedtag") {repl_dict["and_where"]} '
             f'group by 1, 2')
    df = get_bq_data(query, repl_dict)

    cols = ['session_count', 'revenue', 'rps']
    reference_client_bidder_count = 11
    for c in cols:
        df_p = df.pivot(index='client_bidders', columns='bidder', values=c).fillna(0)
        col_order = df_p.loc[8].sort_values(ascending=False).index
        df_p = df_p[col_order]
        fig, ax = plt.subplots(figsize=(16, 12), nrows=2)
        df_p.plot(logy=False, ax=ax[0], title=c)
        df_r = (df_p / df_p.loc[reference_client_bidder_count])
        col_order = df_r.loc[14].sort_values(ascending=False).index
        df_r = df_r[col_order]
        df_r.plot(ax=ax[1])

        fig.savefig(f'plots/bidder_count_by_bidder_{c}.png')

        j = 0


def main_create_optimial_bidder_count_by_bidder(last_date, days):
    query = f'select * from `freestar-157323.ad_manager_dtf.lookup_bidders`'
    df = get_bq_data(query)
    bidders = df.set_index('position')

    repl_dict = {'project_id': project_id,
                 'tablename_from': f'daily_bidder_domain_expt_session_stats_unexpanded_join_{last_date.strftime("%Y-%m-%d")}_{days}_1',
                 'dims': 'date, country_code',
                 'and_where': ' and country_code = "US" and device_category = "desktop"'}

    query = open(os.path.join(sys.path[0], 'queries/query_create_optimial_bidder_count.sql'), "r").read()
    df = get_bq_data(query, repl_dict)

    col_to_plot = 'rps_client'
    col_to_plot = 'session_count_client'

    df = df[['date', 'bidders', col_to_plot]]
    df = df[~df[col_to_plot].isna()]
    df = df.pivot(index='bidders', columns='date', values=col_to_plot).iloc[:, 1:]
    df_all_m = df.mean(axis=1).to_frame('all')
    df_all_s = df.std(axis=1).to_frame('all')

    fig, ax = plt.subplots(figsize=(16, 12))
    df_all_m.plot(ax=ax)
    fig.savefig(f'plots/bidder_count_by_bidder_all_together_{col_to_plot}.png')

    for name, comp in [('only', '='), ('exclude', '!=')]:

        df_solo_list_m = [df_all_m]
        df_solo_list_s = [df_all_s]
        for i in range(2, 21):
            bidder_i = bidders.loc[i, 'bidder']

            if bidder_i in ('ix', 'rise', 'appnexus', 'rubicon', 'triplelift', 'pubmatic', 'sovrn'):
                continue

            repl_dict[
                'and_where'] = f' and country_code = "US" and device_category = "desktop" and substr(fs_clientservermask, {i}, 1) {comp} "2"'
            df = get_bq_data(query, repl_dict)
            df = df[['date', 'bidders', col_to_plot]]
            df = df[~df[col_to_plot].isna()]
            df = df.pivot(index='bidders', columns='date', values=col_to_plot).iloc[:, 1:]
            df_solo_list_m.append(df.mean(axis=1).to_frame(bidder_i))
            df_solo_list_s.append(df.std(axis=1).to_frame(bidder_i))

        df_m = pd.concat(df_solo_list_m, axis=1)
        col_order = df_m.iloc[-1].sort_values(ascending=False).index
        df_m = df_m[col_order]
        df_s = pd.concat(df_solo_list_s, axis=1)
        df_s = df_s[col_order]
        fig, ax = plt.subplots(figsize=(16, 12))
        #        df_m.plot(yerr=df_s, ax=ax, title=f'client bidder count vs rps, with {name} single bidder', xlabel='client bidder count', ylabel='rps')
        df_m.plot(ax=ax, title=f'client bidder count vs rps, with {name} single bidder', xlabel='client bidder count',
                  ylabel='rps')
        fig.savefig(f'plots/bidder_count_by_bidder_{name}_{col_to_plot}.png')

    # exclude teads, justpremium and undertone
    for excl_bidders_list in [['teads', 'justpremium'], ['teads', 'undertone'], ['justpremium', 'undertone'],
                              ['teads', 'justpremium', 'undertone']]:
        excl_bidders_i = [i for i, r in bidders.iterrows() if r['bidder'] in excl_bidders_list]
        excl_bidders_str = ' and '.join([f'substr(fs_clientservermask, {i}, 1) != "2"' for i in excl_bidders_i])
        repl_dict['and_where'] = f' and country_code = "US" and device_category = "desktop" and {excl_bidders_str}'
        df_raw = get_bq_data(query, repl_dict)
        df = df_raw[['date', 'bidders', col_to_plot]]
        df = df[~df[col_to_plot].isna()]
        df = df.pivot(index='bidders', columns='date', values=col_to_plot).iloc[:, 1:]
        df_m = df.mean(axis=1).to_frame('rps')
        df_count = df_raw[['date', 'bidders', 'session_count_client']]
        df_count = df_count[~df_count['session_count_client'].isna()]
        df_count = df_count.pivot(index='bidders', columns='date', values='session_count_client').iloc[:, 1:]

        df_s = (df.std(axis=1) / np.sqrt(df_count.mean(axis=1))).to_frame('rps')

        fig, ax = plt.subplots(figsize=(16, 12))
        df_m.plot(yerr=df_s, ax=ax, title=f'client bidder count vs rps, excluding {", ".join(excl_bidders_list)}',
                  xlabel='client bidder count', ylabel='rps')
        fig.savefig(f'plots/bidder_count_by_bidder_excl_{"_".join(excl_bidders_list)}_{col_to_plot}.png')

    h = 0


if __name__ == "__main__":
    last_date = dt.date(2024, 10, 2)
    days = 10

    # main_create_optimial_bidder_count(last_date, days)

    # main_create_optimial_bidder_count_by_bidder(last_date, days)

#    main_bidder_count(last_date, days)


