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
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()


def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", f'{v}')

    result = client.query(query).result()
    if result is None:
        return

    return result.to_dataframe(bqstorage_client=bqstorageclient)

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

def main_base():

    repl_dict = {'first_date': '2024-11-1',
                 'last_date': '2024-12-10'}

    query_file = 'query_floors_ad_unit_base'
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    get_bq_data(query, repl_dict)

def main():
    #main_ad_unit_filter('ad_unit_name = "/15184186/signupgenius_Desktop_SignUps_Sheet_300x600_Right"')

    for ad_unit_name in ['/22797863291/scribd_incontent_1',
                        '/22797863291/scribd_rightrail_adhesion',
                        '/22797863291/scribd_bottomright_mrec',
                        '/22797863291/scribd_adhesion',
                        '/22797863291/slideshare_adhesion']:

        main_ad_unit_filter(f'ad_unit_name = "{ad_unit_name}"')

def main_ad_unit_filter(ad_unit_filter):

    repl_dict = {'ad_unit_filter': ad_unit_filter}

    query_file = 'query_direct_targetting'
    query = open(os.path.join(sys.path[0], f"queries/{query_file}.sql"), "r").read()
    df = get_bq_data(query, repl_dict)

    plot_specs = [(['optimised_requests'], True), (['optimised_fill_rate'], False), (['optimised_cpm', 'optimised_cpma'], False)]

    fig, ax = plt.subplots(figsize=(16, 12), nrows=len(plot_specs))
    fig.suptitle(f'Flooring data for: {repl_dict["ad_unit_filter"]}')

    for i, (cols, log_y) in enumerate(plot_specs):
        ax_i = ax[i]

        df[cols].plot(ax=ax_i, logy=log_y, style='x-')

    sp = ad_unit_filter.split('"')
    assert len(sp) >= 2
    sp1 = sp[1]
    assert len(sp1) > 2
    sp2 = sp1.split('/')[-1]
    assert len(sp2) > 2
    fig.savefig(f'plots_direct/plot_{sp2}.png')


def main_ad_unit_multiple():

    N = 40
    N_p = 4
    plot_specs = [(['optimised_requests'], True),
                  (['optimised_fill_rate', 'pred_fill_rate'], False),
#                  (['optimised_fill_rate'], False),
                  (['optimised_cpma'], False)]
                  #(['optimised_cpm', 'optimised_cpma'], False)]

    query = f'select ad_unit_name from `streamamp-qa-239417.Floors_2_0.floors_ad_unit_dash` group by 1 order by sum(requests) desc limit {N}'
    df_ad_unit_name = get_bq_data(query)

    n_p = 0
    nn_p = 0
    with PdfPages(f'plots_direct/plots_pdf.pdf') as pdf:

        for i, ad_unit_name in enumerate(df_ad_unit_name['ad_unit_name']):
            if n_p==0:
                fig, ax = plt.subplots(figsize=(25, 20), nrows=N_p, ncols=len(plot_specs))

            repl_dict = {'ad_unit_filter': f'ad_unit_name = "{ad_unit_name}"'}
            query_file = 'query_direct_targetting'
            print(f'doing: {query_file} for: {repl_dict["ad_unit_filter"]}, {i} of {N}')
            df = get_data(query_file, ad_unit_name.replace('/', '_'), repl_dict=repl_dict, force_requery=True)
            df = df.set_index('floor_price')
            df = df[df['optimised_requests'] > 0]

            fit_data = df.reset_index()[['floor_price', 'optimised_fill_rate', 'optimised_requests']].copy()

            # bound_weight = df['optimised_requests'].sum() * 10
            # fit_data_ext = pd.concat([pd.DataFrame([[0, 1, bound_weight]], columns=['floor_price', 'optimised_fill_rate', 'optimised_requests']),
            #                           fit_data,
            #                           pd.DataFrame([[df.index.max() * 1.1, 0, bound_weight]], columns=['floor_price', 'optimised_fill_rate', 'optimised_requests'])])
            # fit_data_ext['ones'] = 1
            # fit_data_ext['floor_price_2'] = fit_data_ext[['floor_price']] ** 2
            # X = fit_data_ext[['ones', 'floor_price', 'floor_price_2']]
            # y = fit_data_ext['optimised_fill_rate']
            # regressor = LinearRegression(fit_intercept=False).fit(X, y, sample_weight=fit_data_ext['optimised_requests'])
            # y_pred = regressor.predict(X)
            # df['pred_fill_rate'] = y_pred[1:-1]

            fit_data_ext = fit_data
            X = fit_data_ext[['floor_price']]
            y = np.log(fit_data_ext['optimised_fill_rate'])
            regressor = LinearRegression(fit_intercept=False).fit(X, y, sample_weight=fit_data_ext['optimised_requests'])
            y_pred = np.exp(regressor.predict(X))
            df['pred_fill_rate'] = y_pred

            for col_i, (cols, log_y) in enumerate(plot_specs):
                df[cols].plot(ax=ax[n_p, col_i], logy=log_y, style='x-')
            ax[n_p, 0].set_ylabel(ad_unit_name.split('/')[-1])
            #ax[n_p, 0].set_title(ad_unit_name.split('/')[-1])

            n_p += 1
            if (n_p==N_p) or (i==len(df_ad_unit_name['ad_unit_name'])-1):
                pdf.savefig()
                fig.savefig(f'plots_direct/plots_png_{nn_p}')
                n_p = 0
                nn_p += 1

if __name__ == "__main__":
    #main_base()
    #main()

    main_ad_unit_multiple()