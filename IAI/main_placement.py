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


def main_raw_dtf_data():

    repl_dict = {'start_date': '2024-12-19',
                 'end_date': '2024-12-26'}

    print(f'running for from {repl_dict["start_date"]} to {repl_dict["end_date"]}')
    query = open(os.path.join(sys.path[0], f"queries/raw_dtf_session_data_all_data.sql"), "r").read()
    get_bq_data(query, repl_dict)

def main_process_data():

    repl_dict = {'start_date': '2024-12-19',
                 'end_date': '2024-12-26'}

    query_filename = 'flying_carpet_analysis'

    for title, where_clause in [('all sessions', ''),
                                ('sessions with only 1 percentile_placement', 'where quantile_placement_count_per_session = 1')]:

        repl_dict['where_clause'] = where_clause
        print(f'running {query_filename} for from {repl_dict["start_date"]} to {repl_dict["end_date"]}')
        query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
        df = get_bq_data(query, repl_dict)

        cols_to_plot = ['all_rps', 'flying_carpet_rps']
        group = df[['quantile_placement'] + cols_to_plot].groupby('quantile_placement')
        mean = group.mean()
        mean_uncertainty = group.std() / np.sqrt(group.count())

        fig, ax = plt.subplots(figsize=(12, 9), nrows=len(cols_to_plot))
        fig.suptitle(f'Effect of quantile_placement for {title}')
        for i, col in enumerate(cols_to_plot):
             mean[col].plot(yerr=mean_uncertainty[col], ax=ax[i], ylabel=col)

        fig.savefig(f'plots/rps_plots_{title[:10].replace(' ', '_')}.png')

    h = 0


def main_process_data_domain():

    repl_dict = {'start_date': '2024-12-19',
                 'end_date': '2024-12-26'}

    query_filename = 'flying_carpet_analysis_with_domain'

    for title, where_clause in [('all sessions', ''),
                                ('sessions with only 1 percentile_placement', 'where quantile_placement_count_per_session = 1')]:

        repl_dict['where_clause'] = where_clause
        print(f'running {query_filename} for from {repl_dict["start_date"]} to {repl_dict["end_date"]}')
        query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
        df = get_bq_data(query, repl_dict)

        cols_to_plot = ['all_rps', 'flying_carpet_rps']

        domain_session_count = df[['domain', 'sessions']].groupby('domain').sum()['sessions']
        domains = domain_session_count.index[domain_session_count > 1000]
        means_dict = dict([(c, {}) for c in cols_to_plot])
        means_uncertainty_dict = dict([(c, {}) for c in cols_to_plot])
        for domain in domains:
            df_d = df[df['domain'] == domain]

            group = df_d[['quantile_placement'] + cols_to_plot].groupby('quantile_placement')
            mean = group.mean()
            mean_uncertainty = group.std() / np.sqrt(group.count())
            for col in cols_to_plot:
                means_dict[col][domain] = mean[col]
                means_uncertainty_dict[col][domain] = mean_uncertainty[col]

        fig, ax = plt.subplots(figsize=(12, 9), nrows=len(cols_to_plot))
        fig.suptitle(f'Effect of quantile_placement for {title}')
        for i, col in enumerate(cols_to_plot):
            means_to_plot = (pd.concat(means_dict[col]).reset_index().rename(columns={'level_0': 'domain'})
                             .pivot(index='quantile_placement', columns='domain', values=col))
            means_uncertainty_to_plot = (pd.concat(means_uncertainty_dict[col]).reset_index().rename(columns={'level_0': 'domain'})
                             .pivot(index='quantile_placement', columns='domain', values=col))
            means_to_plot.plot(yerr=means_uncertainty_to_plot, ax=ax[i], ylabel=col)
        fig.savefig(f'plots/rps_plots_domain_{title[:10].replace(' ', '_')}.png')

        fig, ax = plt.subplots(figsize=(12, 9), nrows=len(cols_to_plot))
        fig.suptitle(f'Effect of quantile_placement for {title}')
        for i, col in enumerate(cols_to_plot):
            means_to_plot = (pd.concat(means_dict[col]).reset_index().rename(columns={'level_0': 'domain'})
                             .pivot(index='quantile_placement', columns='domain', values=col))
            means_uncertainty_to_plot = (
                pd.concat(means_uncertainty_dict[col]).reset_index().rename(columns={'level_0': 'domain'})
                .pivot(index='quantile_placement', columns='domain', values=col))
            norm = means_to_plot.iloc[0]
            (means_to_plot/norm).plot(yerr=means_uncertainty_to_plot/norm, ax=ax[i], ylabel=col)
        fig.savefig(f'plots/rps_plots_domain_norm_{title[:10].replace(' ', '_')}.png')

def main_process_data_page_url():

    repl_dict = {'start_date': '2024-12-19',
                 'end_date': '2024-12-26'}

    query_filename = 'flying_carpet_analysis_with_page_url'

    for title, where_clause in [('all sessions', ''),
                                ('sessions with only 1 percentile_placement',
                                 'where quantile_placement_count_per_session = 1')]:

        repl_dict['where_clause'] = where_clause
        print(f'running {query_filename} for from {repl_dict["start_date"]} to {repl_dict["end_date"]}')
        query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
        df = get_bq_data(query, repl_dict)

        page_url_session_count = df[['page_url', 'sessions']].groupby('page_url').sum()['sessions']
        page_urls = page_url_session_count.index[page_url_session_count > 1000]
        means_dict = {}
        means_uncertainty_dict = {}
        for page_url in page_urls:
            df_d = df[df['page_url'] == page_url]

            group = df_d[['quantile_placement', 'all_rps']].groupby('quantile_placement')
            means_dict[page_url] = group.mean()
            means_uncertainty_dict[page_url] = group.std() / np.sqrt(group.count())

        means_to_plot = (pd.concat(means_dict).reset_index().rename(columns={'level_0': 'page_url'})
                             .pivot(index='quantile_placement', columns='page_url', values='all_rps'))
        means_uncertainty_to_plot = (pd.concat(means_uncertainty_dict).reset_index().rename(columns={'level_0': 'page_url'})
                .pivot(index='quantile_placement', columns='page_url', values='all_rps'))

        df = pd.concat([means_to_plot, means_uncertainty_to_plot]).transpose()
        df.to_csv('plots/page_url_stats.csv')
        h = 0


if __name__ == "__main__":

    #main_raw_dtf_data()

#    main_process_data()

#    main_process_data_domain()

    main_process_data_page_url()




