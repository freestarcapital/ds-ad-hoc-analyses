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

def get_data(query_filename, data_cache_filename, force_requery=False, repl_dict = {}):
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
    for ad_unit_ref in ['all_signupgenius', 'one_ad_unit_signupgenius']:
        for query_file in ['query_fill_rate', 'query_fill_rate_eventstream']:
            for granularity in ['per_day', 'per_hour']:
                main_2(granularity, query_file, ad_unit_ref)

def main_2(granularity='per_day', query_file='query_fill_rate', ad_unit_ref='all_signupgenius'):

    assert granularity in ['per_hour', 'per_day']
    assert query_file in ['query_fill_rate', 'query_fill_rate_eventstream']

    ad_unit_name_match_dict = {'one_ad_unit_signupgenius': 'ad_unit_name = "/15184186/signupgenius_Desktop_SignUps_Sheet_300x600_Right"',
                               'all_signupgenius': 'ad_unit_name like "/15184186/signupgenius%"'}
    ad_unit_name_match = ad_unit_name_match_dict[ad_unit_ref]

    placement_id_match = 'NET.REG_DOMAIN(auc.page_url) like "%signupgenius%"'
    if ad_unit_ref == 'one_ad_unit_signupgenius':
        placement_id_match += ' and placement_id = "signupgenius_Desktop_SignUps_Sheet_300x600_Right"'

    repl_dict = {'first_date': '2024-12-5',
                 'last_date': '2024-12-8',
                 'ad_unit_name_match': ad_unit_name_match,
                 'placement_id_match': placement_id_match,
                 'granularity': granularity}

    if granularity == 'per_day':
        repl_dict['date_hour'] = 'TIMESTAMP_TRUNC(date_hour, day) date_hour'
        repl_dict['N'] = 0
    else:
        repl_dict['date_hour'] = 'date_hour'
        repl_dict['N'] = 23
    if query_file == 'query_fill_rate_eventstream':
        repl_dict['date_hour'] = f'TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), {granularity.replace('per_', '')}) date_hour'

    print(f'Running with query_file: {query_file}, granularity: {granularity}, ad_unit_ref: {ad_unit_ref}')
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    df_all = get_bq_data(query, repl_dict).set_index('date_hour')

    df_all.to_csv(f'data_out/df_all_{ad_unit_ref}_{query_file}_{granularity}.csv')

    for base_col in ['fill_rate', granularity, 'perc']:

        cols = [c for c in df_all.columns if (base_col in c) and ('err' not in c)]
        if len(cols) == 0:
            continue
        df = df_all[cols]

        fig, ax = plt.subplots(figsize=(12, 9))

        err_cols = [c for c in df_all.columns if (base_col in c) and ('err' in c)]
        df_err = df_all[err_cols].rename(columns=dict([(c, c.replace('_err', '')) for c in err_cols]))
        df.plot(ax=ax, title=f'{query_file}, {ad_unit_name_match}, {granularity}', ylabel=base_col, yerr=df_err)

        fig.savefig(f'plots/plot_{ad_unit_ref}_{query_file}_{base_col}_{granularity}.png')



    c = 0

if __name__ == "__main__":

    main()