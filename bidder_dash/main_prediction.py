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
from sklearn.linear_model import LinearRegression

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
    df = client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

    for col in ['date', 'date_hour']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    return df

def get_data_using_query(query, filename, index=None, force_calc=False, repl_dict={}, quiet=False):
    data_cache_filename = f'data_cache/{filename}.pkl'
    if not force_calc and os.path.exists(data_cache_filename):
        if not quiet:
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


def main_prediction():
    query_name = 'query_get_rps_and_uncertainty_day_from_day_table'
    tablename = 'bidder_session_data_raw_domain_day_join_2024-09-05_60_1'

    testgroup = 'experiment'
    repl_dict = {
        'project_id': project_id,
        'select_dimensions': "bidder, country_code, device_category",
        'group_by_dimensions': "bidder, country_code, device_category",
        'tablename': tablename,
        'where': f" and status='client' and fs_testgroup='{testgroup}' and date < '2024-08-28'",
        'N_days_preceding': 1
    }
    query = open(os.path.join(sys.path[0], f'queries/{query_name}.sql'), "r").read()

    df_all = get_data_using_query(query, 'bidder_rps_1', 'date', force_calc=False, repl_dict=repl_dict, quiet=True)

    # with PdfPages(f'plots/predictions_rps.pdf') as pdf:
    #     stats = main_prediction_country(query, repl_dict, pdf=pdf)
    # print(stats)

    cc_dc_list = df_all[['country_code', 'device_category', 'session_count']].groupby(
        ['country_code', 'device_category']).sum().sort_values(by='session_count', ascending=False).index

    stats_list = []
    for (cc, dc) in cc_dc_list[:200]:
        if (len(cc) > 0) and (len(dc) > 0):
            print(f'doing: {cc} {dc}')
            stats = main_prediction_country(query, repl_dict, cc, dc)
            print(stats)
            stats_list.append(stats)

    h = 0

def main_prediction_country(query, repl_dict, country_code='US', device_category='desktop'):

    N_vals = [1, 2, 3, 7]
    df_dict = {}
    for N in N_vals:
        repl_dict['N_days_preceding'] = N
        df_all = get_data_using_query(query, f'bidder_rps_{N}', 'date', force_calc=False, repl_dict=repl_dict, quiet=True)
        df_cc_dc = df_all[(df_all['country_code'] == country_code) & (df_all['device_category'] == device_category)]
        df_dict[N] = df_cc_dc.pivot(columns='bidder', values=['rps', 'session_count'])

    N_max = max(N_vals)

    stats_list = []
    for N, df_N in df_dict.items():
        rps = df_N['rps']
        df_target = rps.shift(-N)
        z = pd.DataFrame({'X': rps[N_max:-N_max].values.flatten(), 'y': df_target[N_max:-N_max].values.flatten()}).dropna()
        reg = LinearRegression(fit_intercept=False).fit(z[['X']], z['y'])
        stats_list.append({'country_code': country_code,
                           'device_category': device_category,
                           'N': N,
                           'R^2': reg.score(z[['X']], z['y']),
                           'coeff': reg.coef_[0],
                           'session_count': df_N['session_count'].mean().mean()})

    stats = pd.DataFrame(stats_list)
    return(stats)







    h = 0

if __name__ == "__main__":
    #main_create_bidder_session_data_raw(datetime.date(2024, 9, 5), days=60)
    #main_country_code(7)
    #main_rolling_hour()

    #main_rolling_day()

    #main_testgroup()

    #main_change()

    #main_domain()

    main_prediction()