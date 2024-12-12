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
        for granularity in ['per_hour', 'per_day']:
            main_2(granularity, ad_unit_ref)

def main_2(granularity='per_day', ad_unit_ref='all_signupgenius'):

    assert granularity in ['per_hour', 'per_day']

    query_file = 'query_fill_rate_price_pressure'

    ad_unit_name_match_dict = {'one_ad_unit_signupgenius': 'ad_unit_name = "/15184186/signupgenius_Desktop_SignUps_Sheet_300x600_Right"',
                               'all_signupgenius': 'ad_unit_name like "/15184186/signupgenius%"'}
    ad_unit_name_match = ad_unit_name_match_dict[ad_unit_ref]

    repl_dict = {'first_date': '2024-12-5',
                 'last_date': '2024-12-7',
                 'ad_unit_name_match': ad_unit_name_match,
                 'granularity': granularity}

    if granularity == 'per_day':
        repl_dict['date_hour'] = 'TIMESTAMP_TRUNC(date_hour, day) date_hour'
        repl_dict['N'] = 0
    else:
        repl_dict['date_hour'] = 'date_hour'
        repl_dict['N'] = 23

    print(f'Running with query_file: {query_file}, granularity: {granularity}, ad_unit_ref: {ad_unit_ref}')
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    df_all = get_bq_data(query, repl_dict).set_index('date_hour')

#    df_all.to_csv(f'data_out/df_all_{ad_unit_ref}_{query_file}_{granularity}.csv')

    fig_specs = {'requests': (['requests', 'impressions', 'revenue'], True),
                 'fill_rate': (['fill_rate'], False),
                 'cpm': (['cpm_'], False),
                 'cpma': (['cpma'], False),
                 'price_pressure': (['price_pressure'], False),
                 'floor_price': (['floor_price'], False)
                 }

    for fig_name, (fig_cols, use_secondary_y) in fig_specs.items():
        fig, ax = plt.subplots(figsize=(12, 9), nrows=len(fig_cols))
        fig.suptitle(fig_name)

        for i, col in enumerate(fig_cols):
            if len(fig_cols) == 1:
                ax_i = ax
            else:
                ax_i = ax[i]

            cols = [c for c in df_all.columns if (col in c) and ('err' not in c) and ('perc' not in c)]
            baseline_cols = [cc for cc in cols if 'baseline' in cc]
            df = df_all[cols]
            err_cols = [c for c in df_all.columns if (col in c) and ('err' in c) and ('perc' not in c)]
            df_err = df_all[err_cols]
            df_err = df_err.rename(columns=dict(zip(err_cols, [c.replace('_err', '') for c in err_cols])))

            col_order = list(df_err.mean().sort_values(ascending=False).index)
            col_order = [c for c in df.columns if c not in col_order] + col_order
            df = df[col_order]

            capsize = 4
            if use_secondary_y:
                df.plot(yerr=df_err, secondary_y=baseline_cols, ylabel=col, ax=ax_i, capsize=capsize)
            else:
                df.plot(yerr=df_err, ylabel=col, ax=ax_i, capsize=capsize)

        fig.savefig(f'plots_fr_pp/plot_fr_pp_{ad_unit_ref}_{fig_name}_{granularity}.png')
        j = 0

def main_dashboard():
    for ad_unit_ref in ['all_signupgenius', 'one_ad_unit_signupgenius']:
        main_dashboard_2(ad_unit_ref)


def main_dashboard_2(ad_unit_ref='all_signupgenius'):

    query_file = 'query_fill_rate_price_pressure_dash'

    ad_unit_name_match_dict = {
        'one_ad_unit_signupgenius': 'ad_unit_name = "/15184186/signupgenius_Desktop_SignUps_Sheet_300x600_Right"',
        'all_signupgenius': 'ad_unit_name like "/15184186/signupgenius%"'}
    ad_unit_name_match = ad_unit_name_match_dict[ad_unit_ref]

    repl_dict = {'first_date': '2024-12-5',
                 'last_date': '2024-12-7',
                 'ad_unit_name_match': ad_unit_name_match,
                 'N': 23}

    print(f'Running with query_file: {query_file}, ad_unit_ref: {ad_unit_ref}')
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    df_all = get_bq_data(query, repl_dict).set_index('date_hour')

    #    df_all.to_csv(f'data_out/df_all_{ad_unit_ref}_{query_file}_{granularity}.csv')

    fig_cols =  ['fill_rate', 'cpma', 'price_pressure', 'floor_price']

    fig, ax = plt.subplots(figsize=(12, 9), nrows=len(fig_cols))

    for i, col in enumerate(fig_cols):
        ax_i = ax[i]
        df = df_all[[c for c in df_all.columns if (col in c)]]
        df.plot(ylabel=col, ax=ax_i)

        fig.savefig(f'plots_fr_pp/dash_fr_pp_{ad_unit_ref}.png')
        j = 0


if __name__ == "__main__":

#    main()
    main_dashboard()
