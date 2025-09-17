import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime as dt
import pickle
import plotly.express as px
import kaleido
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

project_id = "streamamp-qa-239417"
dataset_name = 'DAS_increment'
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_data(query_filename, data_cache_filename=None, force_requery=False, repl_dict={}):

    if data_cache_filename is None:
        data_cache_filename = query_filename
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

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))

    df = client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')
    return df


def get_domains_from_collection_ids(collection_ids, start_date=dt.date.today()-dt.timedelta(days=21), end_date=dt.date.today()):
    min_page_hits = 10000

    print(f'searching for domains for collection_ids: {", ".join(collection_ids)} from {start_date} to {end_date}, min_page_hits: {min_page_hits}')

    repl_dict = {'start_date': start_date.strftime("%Y-%m-%d"),
                 'end_date': end_date.strftime("%Y-%m-%d"),
                 'collection_ids_list': f"('{"', '".join(collection_ids)}')",
                 'min_page_hits': min_page_hits}

    query = open(os.path.join(sys.path[0], "queries/query_get_domains_from_collection_ids.sql"), "r").read()
    df = get_bq_data(query, repl_dict)
    return df['domain'].to_list()


def get_domains_from_test_names(test_names, start_date=dt.date.today()-dt.timedelta(days=21), end_date=dt.date.today()):

    print(f'searching for domains for test_names: {", ".join(test_names)} from {start_date} to {end_date}')

    repl_dict = {'start_date': start_date.strftime("%Y-%m-%d"),
                 'end_date': end_date.strftime("%Y-%m-%d"),
                 'test_names_list': f"('{"', '".join(test_names)}')"}

    query = open(os.path.join(sys.path[0], "queries/query_get_domains_from_test_names.sql"), "r").read()
    df = get_bq_data(query, repl_dict)
    return df['domain'].unique().to_list()


def does_table_exist(tablename):
    query = f"select count(*) from DAS_increment.INFORMATION_SCHEMA.TABLES where table_catalog || '.' || table_schema || '.' || table_name = '{tablename}'"
    df = get_bq_data(query)
    return bool(df.values[0, 0] > 0)


def main(force_recreate_table=True):
    #QUERIES
    query_filename = 'query_BI_AB_test_page_hits'
    #query_filename = 'query_bidder_impact'

    #TIMEOUTS
    # name = 'timeouts'
    # datelist = pd.date_range(start=dt.date(2025,8,26), end=dt.date(2025,9,15))
    # test_domains = get_domains_from_collection_ids(['9c42ef7c-2115-4da9-8a22-bd9c36cdb8b4', '5b60cd25-34e3-4f29-b217-aba2452e89a5'])

    #TRANSPARENT FLOORS
    name = 'transparent_floors'
    datelist = pd.date_range(end=dt.datetime.today().date(), periods=30)
    #datelist = pd.date_range(start=dt.date(2025, 8, 6), end=dt.date(2025, 9, 1))
    test_domains = [
        'pro-football-reference.com',
        'baseball-reference.com',
        'deepai.org',
        'signupgenius.com',
        'perchance.org',
        'worldofsolitaire.com',
        'fantasypros.com',
        'deckshop.pro',
        'tunein.com',
        'adsbexchange.com'
    ]

    #END OF SETUP
    tablename = f"{project_id}.{dataset_name}.{query_filename.replace('query_', '')}_results_{name}"
    first_row = force_recreate_table or (not does_table_exist(tablename))

    domain_list = f"('{"', '".join(test_domains)}')"
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    print(f'query_filename: {query_filename} for domain_list: {domain_list}')

    for date in datelist.tolist():
        create_or_insert_statement = f"delete from `{tablename}` where date = '{date.strftime("%Y-%m-%d")}'; insert into `{tablename}`"
        if first_row:
            create_or_insert_statement = f"CREATE OR REPLACE TABLE `{tablename}` as"
        first_row = False

        print(f'date: {date}: {create_or_insert_statement}')

        repl_dict = {'ddate': date.strftime("%Y-%m-%d"),
                     'create_or_insert_statement': create_or_insert_statement,
                     'domain_list': domain_list,
                     'name': name}
        get_bq_data(query, repl_dict)


def main_data_explore():
    query_filename = 'query_BI_AB_test_page_hits_data_explore'

    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    df = get_bq_data(query)
    df.transpose().to_csv('AB_data_6.csv')

    p = 0

def add_date_cols(df_summary_in, df, val_cols):
    df_summary = df_summary_in.copy()
    for ag in ['min', 'max', 'count']:
        df_summary[f'date_{ag}'] = df[['domain', 'date']].groupby(['domain']).agg(ag)
    df_summary = df_summary[['date_min', 'date_max', 'date_count'] + val_cols]
    return df_summary

def create_table_summary(df, val_cols, calculate_errors_and_t_stats=False):

    df_summary_mean = df[['domain'] + val_cols].groupby(['domain']).agg('mean')
    df_summary_mean_with_dates = add_date_cols(df_summary_mean, df, val_cols)

    if not calculate_errors_and_t_stats:
        return df_summary_mean_with_dates

    df_summary_std = df[['domain'] + val_cols].groupby(['domain']).agg('std')

    summary_mean_error_dict = {}
    for d in df_summary_mean.index:
        summary_mean_error_dict[d] = df_summary_std.loc[d][val_cols] / np.sqrt(df_summary_mean_with_dates.loc[d]['date_count'].astype('float64') - 1)
    df_summary_mean_error = pd.DataFrame(summary_mean_error_dict).transpose()
    df_summary_mean_error_with_dates = add_date_cols(df_summary_mean_error, df, val_cols)

    df_summary_t_stats = df_summary_mean / df_summary_mean_error
    df_summary_t_stats_with_dates = add_date_cols(df_summary_t_stats, df, val_cols)

    return df_summary_mean_with_dates, df_summary_mean_error_with_dates, df_summary_t_stats_with_dates

def main_process_csv():
    query_filename = 'query_get_AB_test_results_for_csv'
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    df_raw = get_bq_data(query)

    index_cols = ['domain', 'date', 'test_name']
    #val_cols = ['session_prop_gam_data']
    val_cols = [c for c in df_raw.columns if c not in index_cols + ['test_group']]
    df = df_raw.pivot(index=index_cols, columns=['test_group'], values=val_cols).reset_index()

    df_uplift = df[index_cols].copy()
    df_uplift.columns = pd.Index(['domain', 'date', 'test_name'], dtype='object')
    for c in val_cols:
        df_uplift[c] = df[c][1].astype('float64') / df[c][0].astype('float64') - 1
    df_uplift = df_uplift.fillna(0)

    summary_mean = create_table_summary(df, val_cols)
    summary_uplift_mean, summary_uplift_error, summary_uplift_t_stats = create_table_summary(df_uplift, val_cols, True)

    df.to_csv('results/details.csv')
    summary_mean.transpose().to_csv('results/summary_mean.csv')
    summary_uplift_mean.transpose().to_csv('results/summary_uplift_mean.csv')
    summary_uplift_error.transpose().to_csv('results/summary_uplift_error.csv')
    summary_uplift_t_stats.transpose().to_csv('results/summary_uplift_t_stats.csv')

    h = 0

if __name__ == "__main__":

    #main()

    main_process_csv()

    #main_data_explore()