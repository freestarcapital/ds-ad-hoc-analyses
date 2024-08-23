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

def get_eventstream_session_data(last_date, days, force_recalc=False):

    repl_dict = {'project_id': project_id,
                 'processing_date': last_date,
                 'days_back_start': days,
                 'days_back_end': 1}

    tablename = f'eventstream_DAS_expt_stats_{repl_dict["processing_date"]}_{repl_dict["days_back_start"]}_{repl_dict["days_back_end"]}'

    if force_recalc:
        print(f'creating table: {tablename}')
        query = open(os.path.join(sys.path[0], "query_eventstream_DAS_expt_stats.sql"), "r").read()
        get_bq_data(query, repl_dict)

    return tablename

def get_data_using_query(query, filename, index=None, force_calc=False, repl_dict={}):
    data_cache_filename = f'data_cache/{filename}.pkl'
    if not force_calc and os.path.exists(data_cache_filename):
        print(f'found existing data file, loading: {data_cache_filename}')
    else:
        print(f'querying to create file: {data_cache_filename}')
        df = get_bq_data(query, repl_dict)
        if index is not None:
            df = df.set_index(index)

        with open(data_cache_filename, 'wb') as f:
            pickle.dump(df, f)

    with open(data_cache_filename, 'rb') as f:
        df = pickle.load(f)
    return df

def get_bidders(force_calc=False):
    query = 'select bidder, position from `freestar-157323.ad_manager_dtf.lookup_bidders` order by 1'
    return get_data_using_query(query, 'bidders', 'bidder', force_calc=force_calc)

def get_mask_values(force_calc=False):
    query = 'select status, mask_value from `freestar-157323.ad_manager_dtf.lookup_mask` order by 2'
    return get_data_using_query(query, 'bidder_mask_values', 'status', force_calc=force_calc)

def main(last_date, days, force_calc_rps_uncertainty=False, force_recalc_eventstream_data=False):

    eventstream_session_data_tablename = get_eventstream_session_data(last_date, days, force_recalc_eventstream_data)
    repl_dict = {'number_of_buckets': 5000,
                 'eventstream_session_data_tablename':  eventstream_session_data_tablename}

    df_bidders = get_bidders()[:2]
    df_mask_values = get_mask_values()

    df_hist_dict = {}
    stats_list = []
    for bidder, bidder_row in df_bidders.iterrows():
        for status, status_row in df_mask_values.iterrows():
            bidder_mask_list = list('.......................')
            bidder_mask_list[bidder_row.position - 1] = str(status_row.mask_value)
            repl_dict['bidder_mask'] = ''.join(bidder_mask_list)

            session_count = get_data_using_query(
                open(os.path.join(sys.path[0], "query_get_bidder_status_session_count.sql"), "r").read(),
                f'bidder_status_session_count_{bidder}_{status}',
                force_calc=force_calc_rps_uncertainty, repl_dict=repl_dict).values[0, 0]

            df_list = []
            for sessions_per_bucket in [20, 100]:#, 500, 2500, 12500]:
                if session_count < sessions_per_bucket * repl_dict['number_of_buckets']:
                    continue

                repl_dict['sessions_per_bucket'] = sessions_per_bucket
                df = get_data_using_query(
                    open(os.path.join(sys.path[0], "query_get_bidder_status_rps.sql"), "r").read(),
                    f'rps_uncertainty_{bidder}_{status}_{sessions_per_bucket}',
                    force_calc=force_calc_rps_uncertainty, repl_dict=repl_dict)

                stats_list.append({'bidder': bidder, 'status': status, 'session_count': session_count, 'sessions': sessions_per_bucket,
                 'mean': df['bucket_rps'].mean(), 'std': df['bucket_rps'].std()})

                df_list.append(df[['bucket_rps']].rename(columns={'bucket_rps': sessions_per_bucket}))

            if len(df_list) == 0:
                continue

            df = pd.concat(df_list, axis=1)
            df_hist_dict[f'{bidder}-{status}'] = {'session_count': session_count, 'df': df}


    # filename = 'bucket_rps'
    # do_plots(df, filename)
    # do_plots(df, filename, True)

def do_plots(df_in, filename, log=False):
    df = df_in.copy()

    if log:
        df = np.log(df)
        xlabel = 'log(rps)'
        filename += '_log'
    else:
        xlabel = 'rps'

    df_mean = df.mean()
    df_std = df.std()

    col_names = [f'{c} sessions, mean: {df_mean[c]:0.2f}, std: {df_std[c]:0.2f}, std/mean: {df_std[c]/df_mean[c]:0.2f}, std/mean*sqrt({c}): {df_std[c] / df_mean[c] * np.sqrt(c):0.2f}'
                 for c in df.columns]
    df_max = 15
    df[df > df_max] = df_max
    y_pdf, x_pdf, _ = plt.hist(df, 100, density=True)
    df_hist_pdf = pd.DataFrame(y_pdf.transpose(), index=pd.Index(x_pdf[:-1]), columns=col_names)
    df_hist_pdf['mean'] = (df_hist_pdf.index >= df_mean.iloc[-1]) * y_pdf.max()
    y_cdf, x_cdf, _ = plt.hist(df, 100, density=True, cumulative=True)
    df_hist_cdf = pd.DataFrame(y_cdf.transpose(), index=pd.Index(x_cdf[:-1]), columns=col_names)
    df_hist_cdf['mean'] = (df_hist_cdf.index >= df_mean.iloc[-1]) * y_cdf.max()
    fig, ax = plt.subplots(figsize=(12, 9), nrows=2)
    df_hist_pdf.plot(ax=ax[0], title='Histogram of rps measured over different session lengths (shown in legend)', xlabel=xlabel, ylabel='pdf')
    df_hist_cdf.plot(ax=ax[1], xlabel=xlabel, ylabel='cdf')

    fig.savefig(f'plots/{filename}.png')

    h = 0


if __name__ == "__main__":

    main(last_date=datetime.date(2024, 8, 20), days=30, force_recalc_eventstream_data=False)
