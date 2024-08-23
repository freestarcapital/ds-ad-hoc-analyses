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

def get_df_stats_and_df_hist_dict(last_date, days, number_of_buckets=1000, modifications=(""),
                                  force_calc_rps_uncertainty=False, force_recalc_eventstream_data=False):
    amazon_and_preGAM = ['amazon', 'preGAMAuction']
    amazon_and_preGAM_client = False

    and_filter_string = ""
    filename_filter_string = ""
    if len(modifications[0]) > 0:
        filename_filter_string = modifications[0]
        and_filter_string = modifications[1]
        amazon_and_preGAM_client = modifications[2]

    eventstream_session_data_tablename = get_eventstream_session_data(last_date, days, force_recalc_eventstream_data)
    repl_dict = {'number_of_buckets': number_of_buckets,
                 'eventstream_session_data_tablename':  eventstream_session_data_tablename,
                 'and_filter_string': and_filter_string}

    df_bidders = get_bidders(force_calc=force_calc_rps_uncertainty)
    df_mask_values = get_mask_values(force_calc=force_calc_rps_uncertainty)

    df_hist_dict = {}
    stats_list = []
    for bidder, bidder_row in df_bidders.iterrows():
        for status, status_row in df_mask_values.iterrows():
            bidder_mask_list = list('.......................')
            if amazon_and_preGAM_client:
                if bidder in amazon_and_preGAM:
                    f'doing amazon_and_preGAM_client, so skipping {bidder}'
                    continue
                for bidder_to_set_to_client in amazon_and_preGAM:
                    bidder_mask_list[df_bidders.loc[bidder_to_set_to_client]['position']-1] = str(df_mask_values.loc['client']['mask_value'])

            bidder_mask_list[bidder_row.position - 1] = str(status_row.mask_value)
            repl_dict['bidder_mask'] = ''.join(bidder_mask_list)

            session_count = get_data_using_query(
                open(os.path.join(sys.path[0], "query_get_bidder_status_session_count.sql"), "r").read(),
                f'bidder_status_session_count_{bidder}_{status}{filename_filter_string}',
                force_calc=force_calc_rps_uncertainty, repl_dict=repl_dict).values[0, 0]

            df_list = []
            for sessions_per_bucket in [20, 100, 500, 2500, 12500]:
                if session_count < sessions_per_bucket * repl_dict['number_of_buckets']:
                    continue

                repl_dict['sessions_per_bucket'] = sessions_per_bucket
                df = get_data_using_query(
                    open(os.path.join(sys.path[0], "query_get_bidder_status_rps.sql"), "r").read(),
                    f'rps_uncertainty_{bidder}_{status}_{sessions_per_bucket}_{repl_dict["number_of_buckets"]}{filename_filter_string}',
                    force_calc=force_calc_rps_uncertainty, repl_dict=repl_dict)

                stats_list.append({'bidder': bidder, 'status': status, 'modification': filename_filter_string, 'session_count': session_count, 'sessions': sessions_per_bucket,
                         'mean': df['bucket_rps'].mean(), 'std': df['bucket_rps'].std()})

                df_list.append(df[['bucket_rps']].rename(columns={'bucket_rps': sessions_per_bucket}))

            if len(df_list) == 0:
                continue

            df = pd.concat(df_list, axis=1)
            df_hist_dict[f'{bidder}-{status}'] = {'session_count': session_count, 'modification': filename_filter_string, 'df': df}

    df_stats = pd.DataFrame(stats_list)
    return df_stats, df_hist_dict, filename_filter_string

def do_hist_plots(df_in, bidder_status, session_count=0, log=False, pdf=None):
    df = df_in.copy()

    filename = bidder_status
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
    df_hist_pdf.plot(ax=ax[0], title=f'Histogram of rps for {bidder_status}, session count: {session_count/1e6:0.2f}M', xlabel=xlabel, ylabel='pdf')
    df_hist_cdf.plot(ax=ax[1], xlabel=xlabel, ylabel='cdf')

    fig.savefig(f'plots/bidder_status_rps_uncertainty_pngs/{filename}.png')
    if pdf is not None:
        pdf.savefig()

def client_server_count_and_modification(max_client_count_from_8, max_server_count_from_5):

    if max_client_count_from_8 == 0:
        str = f" and array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '2')) = 8"
    else:
        str = f" and abs(array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '2')) - 8) <= {max_client_count_from_8}"

    if max_server_count_from_5 == 0:
        str += f" and array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '3')) = 5"
    else:
        str += f" and abs(array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '3')) - 5) <= {max_server_count_from_5}"

    return str

def main(last_date, days, force_calc_rps_uncertainty=False, force_recalc_eventstream_data=False):
    number_of_buckets = 200
    do_plots = False

    df_stats_list = []
    #(name, additional and in where clause, amazon_and_preGAM_client)
    US_desktop = "and country_code='US' and device_category='desktop'"

    for modifications in [
        ("", "", False),
        ("_US_desktop", "and country_code='US' and device_category='desktop'", False),
        ("_amazon_preGAMAuction_client", "", True),
        ("_US_desktop_amazon_preGAMAuction_client", US_desktop, True),
        ("_US_desktop_8_5_US_desktop_apGc", US_desktop + client_server_count_and_modification(0, 0), True),
        ("_US_desktop_789_456_US_desktop_apGc", US_desktop + client_server_count_and_modification(1, 1), True),
        ("_US_desktop_678910_34567_US_desktop_apGc", US_desktop + client_server_count_and_modification(2, 2), True)]:

        df_stats, df_hist_dict, filename_filter_string = get_df_stats_and_df_hist_dict(last_date, days, number_of_buckets,
            modifications, force_calc_rps_uncertainty, force_recalc_eventstream_data)

        if do_plots:
            with PdfPages(f'plots/bidder_status_rps_uncertainty_{number_of_buckets}{filename_filter_string}.pdf') as pdf:
                for bidder_status, data in df_hist_dict.items():
                    do_hist_plots(data['df'], bidder_status, data['session_count'], pdf=pdf)

        df_stats_list.append(df_stats)

    df_stats = pd.concat(df_stats_list)
    df_stats['std_over_mean'] = df_stats['std'] / df_stats['mean']
    df_stats['std_over_mean_times_sqrt_sessions'] = df_stats['std_over_mean'] * np.sqrt(df_stats['sessions'])
    df_stats.to_csv(f'plots/bidder_status_rps_uncertainty_stats_{number_of_buckets}.csv')

    df_stats_no_disables = df_stats[df_stats['status'] != 'disabled']
    for analysis_columns in [['sessions', 'modification'], ['sessions', 'status', 'modification']]:
        results = df_stats_no_disables[['mean', 'std', 'std_over_mean_times_sqrt_sessions'] + analysis_columns].groupby(analysis_columns).mean()
        results.to_csv(f'plots/bidder_status_analysis_{number_of_buckets}_{'_'.join(analysis_columns)}.csv')

if __name__ == "__main__":
    main(last_date=datetime.date(2024, 8, 20), days=30)
