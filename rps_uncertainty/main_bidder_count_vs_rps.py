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

    df = client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')
    for col in ['date', 'date_hour']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    return df

def main():

#    repl_dict = {'table_ext': '2024-08-20_30_1'}
    repl_dict = {'table_ext': '2024-09-05_7_1',
                # 'DTF_or_eventstream': 'eventstream'}
                 'DTF_or_eventstream': 'DTF'}

    query = open(os.path.join(sys.path[0], 'query_bidder_count_vs_rps.sql'), "r").read()
    df = get_bq_data(query, repl_dict)
    df = df.set_index('bidders')
    df = df.iloc[:, :6]

    fig, ax = plt.subplots(figsize=(12, 9))
    df_avg_cols = [c for c in df.columns if '_err_' not in c]
    df_avg = df[df_avg_cols]
    df_avg_err_cols = [c for c in df.columns if '_err_' in c]
    df_avg_err = df[df_avg_err_cols]
    df_avg_err = df_avg_err.rename(columns=dict(zip(df_avg_err_cols, df_avg_cols)))

    df_avg.plot(style='x-', ylabel='rps', title='mask bidder count vs average rps', ax=ax, yerr=df_avg_err)
    fig.savefig(f'plots/bidder_count_vs_rps_{repl_dict['DTF_or_eventstream']}_{repl_dict['table_ext']}_{datetime.datetime.today().strftime("%Y-%m-%d")}.png')


def main_country():

#    repl_dict = {'table_ext': '2024-08-20_30_1'}
    repl_dict = {'table_ext': '2024-09-05_7_1',
                 'DTF_or_eventstream': 'eventstream'}
                # 'DTF_or_eventstream': 'DTF'}

#    panels = ['client_bidders', 'server_bidders', 'all_bidders']
    panels = ['client_bidders', 'all_bidders']

    include_counts = False
    N = 10
    query = open(os.path.join(sys.path[0], 'query_bidder_count_vs_rps_country_or_device.sql'), "r").read()

    for cd in ['device_category', 'country_code']:
        repl_dict['country_or_device'] = cd

        if include_counts:
            fig, ax = plt.subplots(figsize=(20, 16), ncols=len(panels), nrows=2)
        else:
            fig, ax = plt.subplots(figsize=(16, 12), ncols=len(panels))

        for i, bidders in enumerate(panels):
            repl_dict['which_bidders'] = bidders
            df = get_bq_data(query, repl_dict)
            df = df[df[bidders] > 0]
            top_N = df[[cd, 'count']].groupby(cd).sum().sort_values('count', ascending=False)[:N].index

            df_p = df.pivot(index=bidders, columns=cd, values='rps')
            df_p = df_p[top_N]
            col_order = df_p.iloc[0].sort_values(ascending=False).index
            df_p = df_p[col_order]

            df_p_err = df.pivot(index=bidders, columns=cd, values='rps_err')
            df_p_err = df_p_err[top_N]
            df_p_err = df_p_err[col_order]

            if include_counts:
                df_p.plot(style='x-', ylabel='rps', ax=ax[0, i], title=bidders, yerr=df_p_err)

                df_p = df.pivot(index=bidders, columns=cd, values='count')
                df_p = df_p[top_N]
                df_p[col_order].plot(style='x-', ylabel='count', ax=ax[1, i], title=bidders, logy=True)
            else:
                df_p.plot(style='x-', ylabel='rps', ax=ax[i], title=bidders, yerr=df_p_err)


        fig.savefig(f'plots/bidder_count_vs_rps_{cd}_{repl_dict['DTF_or_eventstream']}_{repl_dict['table_ext']}_{datetime.datetime.today().strftime("%Y-%m-%d")}.png')

    a=0

def main_by_day():
    repl_dict = {'table_ext': '2024-10-7_60_1',
                 'DTF_or_eventstream': 'DTF'}
    query = open(os.path.join(sys.path[0], 'query_bidder_count_vs_rps_by_day.sql'), "r").read()
    query_session_count = open(os.path.join(sys.path[0], 'query_bidder_count_vs_rps_session_count.sql'), "r").read()

    for (where_raw, title_raw) in [("country_code = 'US' and device_category = 'desktop'", 'US desktop'),
                           ("country_code = 'US' and device_category != 'desktop'", 'US non desktop'),
                           ("country_code = 'US'", 'US'),
                           ("country_code != 'US'", 'non US')]:

        for rtt in [None]:#, 'slow', 'medium', 'fast', 'superfast']:

            where = where_raw
            title = title_raw
            if rtt is not None:
                where += f" and rtt_category = '{rtt}'"
                title += f" rtt {rtt}"

            print(f'doing: {title}')

            repl_dict['and_where'] = ' and ' + where
            df_all = get_bq_data(query, repl_dict)
            if df_all.empty:
                print(f'no data for {title}, skipping')
                continue

            df_all = df_all[(df_all['date'] >= '2024-08-17') & (df_all['date'] != '2024-08-28') & (df_all['date'] != '2024-09-24')]

            cols_to_plot = ['rps_client', 'rps_server', 'rps_client_server']
            for i, col in enumerate(cols_to_plot):

                plot_title = f'Experiment rps for {title} for {col.replace("rps_","")}'

                df = df_all[['bidders', 'date'] + [col]].pivot(index='bidders', columns='date', values=col)
                col_err = col + '_err'
                df_err = df_all[['bidders', 'date'] + [col_err]].pivot(index='bidders', columns='date', values=col_err)
                fig, ax = plt.subplots(figsize=(12, 9))
                df.plot(ax=ax, ylabel=col, yerr=df_err, title=plot_title)
                fig.savefig(f'plots/rps_count_date_{title.replace(' ', '_')}_{col}_{datetime.datetime.today().strftime("%Y-%m-%d")}.png')

                df_split = {'to_Aug28': df.loc[:, df.columns <= '2024-08-27'],
                            'from_Aug29_to_Sep23': df.loc[:, (df.columns >= '2024-08-29') & (df.columns <= '2024-09-23')],
                            # 'from_Aug29_to_Sep03': df.loc[:, (df.columns >= '2024-08-29') & (df.columns <= '2024-09-03')],
                            # 'from_Aug04_to_Sep17': df.loc[:, (df.columns >= '2024-09-04') & (df.columns <= '2024-09-17')],
                            # 'from_Sep18_to_Sep23': df.loc[:, (df.columns >= '2024-09-18') & (df.columns <= '2024-09-23')],
                            'from_Sep25': df.loc[:, df.columns >= '2024-09-25']}

                df_list = []
                df_err_list = []
                for name, df_d in df_split.items():
                    df_list.append(df_d.mean(axis=1).to_frame(name))
                    df_err_list.append(np.sqrt(((df_d ** 2).mean(axis=1) - df_d.mean(axis=1) ** 2) / (len(df_d.columns)-1)).to_frame(name))

                df_j = pd.concat(df_list, axis=1)
                df_j = df_j.loc[df_j.sum(axis=1) > 0]
                df_j_err = pd.concat(df_err_list, axis=1)
                df_j_err = df_j_err.loc[df_j_err.sum(axis=1) > 0]

                fig, ax = plt.subplots(figsize=(12, 9))
                df_j.plot(ax=ax, ylabel=col, yerr=df_j_err, title=plot_title)#, ylim=[15, 25])
                if col == 'rps_client':
                    df_session_count = get_bq_data(query_session_count, repl_dict)
                    df_session_count = df_session_count.set_index('client_bidders')
                    df_session_count = df_session_count.loc[df_session_count.index <= df_j.index.max()]
                    df_session_count.plot(ax=ax, secondary_y=True, ylabel='sessions', style='x-')

                fig.savefig(f'plots/rps_count_date_joint_{title.replace(' ', '_')}_{col}_{datetime.datetime.today().strftime("%Y-%m-%d")}.png')


def main_opt_over_time():

    for (where_raw, title_raw) in [('country_code = "US" and device_category = "desktop"', 'US desktop'),
                           ('country_code = "US" and device_category != "desktop"', 'US non desktop'),
                           ("country_code = 'US'", 'US'),
                           ('country_code != "US"', 'non US')]:

        for rtt in [None]:#, 'slow', 'medium', 'fast', 'superfast']:

            where = where_raw
            title = title_raw
            if rtt is not None:
                where += f' and rtt_category = "{rtt}"'
                title += f' rtt {rtt}'

            print(f'doing: {title}')

            query = ('select date, avg(revenue) * 1000 rps, '
                     'avg(array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), "2")) + '
                            'if(date >= "2024-08-28", 6, 0) + if(date >= "2024-09-24", 7, 0)) AS client_bidders, '
                     'avg(array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), "3"))) AS server_bidders '
                     'from `streamamp-qa-239417.DAS_eventstream_session_data.DTF_DAS_opt_stats_split_revenue_2024-10-7_60_1` '
                     f'where (fs_clientservermask is not null) and char_length(fs_clientservermask) = 23 '
                     "and regexp_contains(fs_clientservermask, '[0123]{23}') "
                     "and substr(fs_clientservermask, 10, 1) in ('0', '1') and substr(fs_clientservermask, 11, 1) in ('0', '1') "
                     "and substr(fs_clientservermask, 21, 1) in ('0', '1') "
                     "and substr(fs_clientservermask, 22, 1) in ('0', '1') "
                     "and substr(fs_clientservermask, 17, 1) in ('0', '1') and substr(fs_clientservermask, 18, 1) in ('0', '1') "
                     "and substr(fs_clientservermask, 19, 1) in ('0', '1')"
                     #"and substr(fs_clientservermask, 13, 1) in ('0', '1')"
                     f'and {where} '
                     'group by 1 order by 1')

            df = get_bq_data(query)
            if df.empty:
                f'no data found for {title}, skipping'
                continue

            df = df[(df['date'] >= '2024-08-17') & (df['date'] != '2024-08-28') & (df['date'] != '2024-09-24')]
            df = df.set_index('date')

            df_dict = {'to_Aug28': df[df.index < '2024-08-28']['rps'],
                       'Aug29_Sep23': df[(df.index >= '2024-08-29') & (df.index <= '2024-09-23')]['rps'],
                       # 'Aug29_Sep3': df[(df.index >= '2024-08-29') & (df.index <= '2024-09-03')]['rps'],
                       # 'Aug4_Sep17': df[(df.index >= '2024-09-04') & (df.index <= '2024-09-17')]['rps'],
                       # 'Sep18_Sep23': df[(df.index >= '2024-09-18') & (df.index <= '2024-09-23')]['rps'],
                       'from_Sep25': df[df.index >= '2024-09-25']['rps']}

            plot_title = title
            for name, df_d in df_dict.items():
                plot_title += f', {name} rps: {df_d.mean():0.1f} +/- {np.sqrt(((df_d ** 2).mean() - df_d.mean() ** 2) / (len(df_d) - 1)):0.1f}'

            fig, ax = plt.subplots(figsize=(16, 12))
            df[['rps']].plot(ax=ax, title=f'Optimised rps for {plot_title}', ylabel='rps', style='x-')
            df[['client_bidders', 'server_bidders']].plot(ax=ax, ylabel='bidder count', style='x-',
                    secondary_y=['client_bidders', 'server_bidders'])
            fig.savefig(f'plots/rps_opt_over_time_{title.replace(" ", "_")}_{datetime.datetime.today().strftime("%Y-%m-%d")}.png')




if __name__ == "__main__":
    #main()
    #main_country()

    main_opt_over_time()
    main_by_day()