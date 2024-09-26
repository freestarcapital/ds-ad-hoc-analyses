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

fixed_bidders = ['ix', 'rise', 'appnexus', 'rubicon', 'triplelift', 'pubmatic']

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{"+k+"}", f'{v}')
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')


def get_data_using_query(query, filename, index=None, force_calc=False, repl_dict={}):
    data_cache_filename = f'data_cache/wb_{filename}.pkl'
    if not force_calc and os.path.exists(data_cache_filename):
        print(f'found existing data file, loading: {data_cache_filename}')
    else:
        print(f'{datetime.datetime.now()}: querying to create file: {data_cache_filename}')
        df = get_bq_data(query, repl_dict)
        if index is not None:
            df = df.set_index(index)

        with open(data_cache_filename, 'wb') as f:
            pickle.dump(df, f)

    with open(data_cache_filename, 'rb') as f:
        df = pickle.load(f)
    return df


def main_create_bidder_session_data_raw(last_date=datetime.date(2024, 9, 3), days=30):

    repl_dict = {'project_id': project_id,
                 'processing_date': last_date,
                 'days_back_start': days,
                 'days_back_end': 1,
                # 'aer_to_bwr_join_type': 'left join'
                 'aer_to_bwr_join_type': 'join'}

    query = open(os.path.join(sys.path[0], 'queries/query_bidder_session_data_raw_domain_day.sql'), "r").read()
    get_bq_data(query, repl_dict)

    # query = open(os.path.join(sys.path[0], 'queries/query_bidder_session_data_raw.sql'), "r").read()
    # get_bq_data(query, repl_dict)

    # query = open(os.path.join(sys.path[0], 'queries/query_bidder_session_data_raw_domain.sql'), "r").read()
    # get_bq_data(query, repl_dict)

def main_country_code(rolling_days=1):

    repl_dict = {
        'project_id': project_id,
        'select_dimensions': 'bidder, country_code',
        'group_by_dimensions': 'bidder, country_code',
        'tablename': 'bidder_session_data_raw_domain_day_join_2024-09-05_20_1',
        'where': f" and status = 'client' and fs_testgroup = 'experiment'",
        'N_days_preceding': rolling_days-1
    }

    query = open(os.path.join(sys.path[0], 'queries/query_get_rps_and_uncertainty_day_from_day_table.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df['date'] = pd.to_datetime(df['date'])

    country_codes = df[['country_code', 'session_count']].groupby('country_code').sum().sort_values('session_count', ascending=False).index[:30]

    with PdfPages(f'plots/rps_country_code_rolling_{repl_dict["N_days_preceding"]+1}.pdf') as pdf:
        for cc in country_codes:

            df_cc = df[df['country_code'] == cc]

            df_p_dict = {}
            for value in ['session_count', 'rps', 'rps_std']:
                df_p = df_cc.pivot(index='date', columns='bidder', values=value).fillna(0)
                df_p = df_p.iloc[1:, :]
                df_p = df_p[df_p.iloc[-1, :].sort_values(ascending=False).index]
                df_p_dict[value] = df_p

            fig, ax = plt.subplots(figsize=(16, 12), ncols=2)
            fig.suptitle(f'{cc} desktop, rolling {repl_dict["N_days_preceding"]+1} day average')
            df_p_dict['rps'].plot(yerr=df_p_dict['rps_std'], ax=ax[0], ylabel='bidder rps with uncertainty')
            df_p_dict['session_count'].plot(ax=ax[1], logy=True, ylabel='session count')

            fig.savefig(f'plots/country_pngs/rps_{cc}_rolling_{repl_dict["N_days_preceding"]+1}.png')
            pdf.savefig()


def main_rolling():

    domain_table = '_domain'
    domain_table = ''

    repl_dict = {
        'project_id': project_id,
        'select_dimensions': 'bidder',
        'group_by_dimensions': 'bidder',
        'tablename': f'bidder_session_data_raw{domain_table}_ join _2024-09-01_30_1',
        'where': f"where status = 'client' and bidder not in ('amazon', 'preGAMAuction') and country_code='US' and device_category='desktop' and fs_testgroup = 'experiment'"
    }
    query = open(os.path.join(sys.path[0], 'queries/query_get_rps_and_uncertainty_hour.sql'), "r").read()

    with PdfPages(f'plots/rps_US_desktop_hour_rolling_hours{domain_table}.pdf') as pdf:
        for H in [1, 2, 6, 24]:

            repl_dict['N_hours_preceding'] = H-1
            df = get_bq_data(query, repl_dict)
            df['date_hour'] = pd.to_datetime(df['date_hour'])

            df_p_dict = {}
            for value in ['session_count', 'rps', 'rps_std']:
                df_p = df.pivot(index='date_hour', columns='bidder', values=value).fillna(0).sort_index()
                df_p = df_p.iloc[H:, :]
                df_p = df_p['2024-08-20':'2024-08-28']
                df_p = df_p[df_p.iloc[-1, :].sort_values(ascending=False).index]
                df_p_dict[value] = df_p

            fig, ax = plt.subplots(figsize=(16, 12), ncols=2)
            fig.suptitle(f'US desktop, rolling hours: {H}')
            df_p_dict['rps'].plot(yerr=df_p_dict['rps_std'], ax=ax[0], ylabel='rps with uncertainty')
            df_p_dict['session_count'].plot(ax=ax[1], logy=True, ylabel='session count')

            pdf.savefig()

def main_rolling_day():

    domain_table = '_domain'

    repl_dict = {
        'project_id': project_id,
        'select_dimensions': 'bidder',
        'group_by_dimensions': 'bidder',
        'tablename': f'bidder_session_data_raw{domain_table}_ join _2024-09-01_30_1',
        'where': f"where status = 'client' and bidder not in ('amazon', 'preGAMAuction') and country_code='US' and device_category='desktop' and fs_testgroup = 'experiment'"
    }
    query = open(os.path.join(sys.path[0], 'queries/query_get_rps_and_uncertainty_day.sql'), "r").read()

    with PdfPages(f'plots/rps_US_desktop_hour_rolling_days{domain_table}.pdf') as pdf:
        for H in [1, 2, 4, 7]:

            repl_dict['N_days_preceding'] = H-1
            df = get_bq_data(query, repl_dict)
            df['date'] = pd.to_datetime(df['date'])

            df_p_dict = {}
            for value in ['session_count', 'rps', 'rps_std']:
                df_p = df.pivot(index='date', columns='bidder', values=value).fillna(0).sort_index()
                df_p = df_p.iloc[H:, :]
                df_p = df_p[:'2024-08-28']
                df_p = df_p[df_p.iloc[-1, :].sort_values(ascending=False).index]
                df_p_dict[value] = df_p

            fig, ax = plt.subplots(figsize=(16, 12), ncols=2)
            fig.suptitle(f'US desktop, rolling hours: {H}')
            df_p_dict['rps'].plot(yerr=df_p_dict['rps_std'], ax=ax[0], ylabel='rps with uncertainty')
            df_p_dict['session_count'].plot(ax=ax[1], logy=True, ylabel='session count')

            pdf.savefig()


def main_testgroup():

    repl_dict = {
        'project_id': project_id,
        'select_dimensions': "concat(bidder, '-', status) as bidder, fs_testgroup",
        #'select_dimensions': "bidder, fs_testgroup",
        'group_by_dimensions': 'bidder, fs_testgroup',
        'tablename': f'bidder_session_data_raw_domain_join_2024-09-05_20_1',
        'where': f" where status in ('client', 'server') "
                 f" and bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi', 'insticator') "
                 f" and country_code='US' and device_category='desktop'",
        'N_days_preceding': 0
    }
    query = open(os.path.join(sys.path[0], 'queries/query_get_rps_and_uncertainty_day_from_day_table.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df['date'] = pd.to_datetime(df['date'])

    N = 5
    bidders = df['bidder'].unique()
    bidders.sort()
    with PdfPages(f'plots/rps_testgroup_days.pdf') as pdf:
        i = 0
        for b in bidders:
            if i == 0:
                fig, ax = plt.subplots(figsize=(16, 12), nrows=N)
                fig.suptitle(f'US desktop, client bidders')

            df_b = df[df['bidder'] == b]
            df_p_dict = {}
            for value in ['session_count', 'rps', 'rps_std']:
                df_p = df_b.pivot(index='date', columns='fs_testgroup', values=value).sort_index()
                df_p = df_p.iloc[1:, :]
                df_p_dict[value] = df_p

            title = b
            if b.split('-')[0] in fixed_bidders:
                title += ' *'
            df_p_dict['rps'].plot(yerr=df_p_dict['rps_std'], ax=ax[i], ylabel=title)

            i += 1
            if (i == N) or (b == bidders[-1]):
                i = 0
                pdf.savefig()

def main_change():
    # day_or_hour = 'hour'
    # days_or_hours_smoothing = 24
    day_or_hour = 'day'

    for days_or_hours_smoothing in [1, 7]:

        main_bidders(day_or_hour=day_or_hour, days_or_hours_smoothing=days_or_hours_smoothing)
        main_bidders('US desktop', "and country_code='US' and device_category = 'desktop'",
                     day_or_hour=day_or_hour, days_or_hours_smoothing=days_or_hours_smoothing)


def main_bidders(title='all', additional_where="", day_or_hour='day', days_or_hours_smoothing=7):

    if day_or_hour == 'day':
        time_col = 'date'
        query_name = 'query_get_rps_and_uncertainty_day_from_day_table'
        tablename = 'bidder_session_data_raw_domain_day_join_2024-09-05_20_1'
    elif day_or_hour == 'hour':
        time_col = 'date_hour'
        query_name = 'query_get_rps_and_uncertainty_hour'
        tablename = 'bidder_session_data_raw_domain_join_2024-09-05_20_1'
    else:
        assert False

    repl_dict = {
        'project_id': project_id,
        'select_dimensions': "bidder, fs_testgroup",
        'group_by_dimensions': 'bidder, fs_testgroup',
        'tablename': tablename,
        'where': f" and status='client' {additional_where}",
        f'N_{day_or_hour}s_preceding': days_or_hours_smoothing-1
    }
    query = open(os.path.join(sys.path[0], f'queries/{query_name}.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df[time_col] = pd.to_datetime(df[time_col])

    fig, ax = plt.subplots(figsize=(16, 12), ncols=2)
    fig.suptitle(f'{title}, rolling {days_or_hours_smoothing}{day_or_hour}s')

    for i, tg in enumerate(['experiment', 'optimised']):
        df_tg = df[df['fs_testgroup'] == tg]
        df_p_dict = {}
        for value in ['session_count', 'rps', 'rps_std']:
            df_p = df_tg.pivot(index=time_col, columns='bidder', values=value).sort_index()
            df_p = df_p.iloc[repl_dict[f'N_{day_or_hour}s_preceding']+1:, :]
            df_p = df_p[df_p.iloc[-1, :].sort_values(ascending=False).index]
            df_p.columns = [f"{c}{' *' if c in fixed_bidders else ''}" for c in df_p.columns]
            df_p_dict[value] = df_p
        df_p_dict['rps'].plot(yerr=df_p_dict['rps_std'], ax=ax[i], title=tg, ylabel='bidder rps')
    fig.savefig(f'plots/rps_over_change_{title.replace(' ','_')}_{day_or_hour}_{days_or_hours_smoothing}.png')


def main_domain():

    repl_dict = {
        'project_id': project_id,
        'select_dimensions': 'bidder, domain',
        'group_by_dimensions': 'bidder, domain',
        'tablename': 'bidder_session_data_raw_domain_day_join_2024-09-16_10_1',
        'where': f" and status = 'client' and fs_testgroup='optimised'",
        'N_days_preceding': 0
    }

    query = open(os.path.join(sys.path[0], 'queries/query_get_rps_and_uncertainty_day_from_day_table.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df['date'] = pd.to_datetime(df['date'])

    domains = df[['domain', 'session_count']].groupby('domain').sum().sort_values('session_count', ascending=False).index[:50]

    df_domains = df[[b in domains for b in df['domain']]]
    df_domains.sort_values(['domain', 'bidder', 'date']).to_csv('plots/rps_domains.csv')

    with PdfPages(f'plots/rps_domains.pdf') as pdf:
        for d in domains:

            df_d = df[df['domain'] == d]

            df_p_dict = {}
            for value in ['session_count', 'rps', 'rps_std']:
                df_p = df_d.pivot(index='date', columns='bidder', values=value).fillna(0)
                df_p = df_p.iloc[1:, :]
                df_p = df_p[df_p.iloc[-1, :].sort_values(ascending=False).index]
                df_p_dict[value] = df_p

            fig, ax = plt.subplots(figsize=(12, 9))
            fig.suptitle(d)
            df_p_dict['rps'].plot(yerr=df_p_dict['rps_std'], ax=ax, ylabel='bidder rps with uncertainty')
            #df_p_dict['session_count'].plot(ax=ax[1], logy=True, ylabel='session count')

            fig.savefig(f'plots/domain_pngs/rps_{d}.png')
            pdf.savefig()

if __name__ == "__main__":
    main_create_bidder_session_data_raw(datetime.date(2024, 9, 23), days=30)
    #main_country_code(7)
    #main_rolling_hour()

    #main_rolling_day()

    #main_testgroup()

    #main_change()

    #main_domain()

