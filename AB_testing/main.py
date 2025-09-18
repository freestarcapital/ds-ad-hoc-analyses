import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime as dt
import pickle
import numpy as np
import xlsxwriter
from xlsxwriter.color import Color

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


def get_domains_from_collection_ids(collection_ids, datelist):
    min_page_hits = 10000

    start_date = datelist[0]
    end_date = datelist[-1] + dt.timedelta(days=1)
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
    # test_domains = get_domains_from_collection_ids(['9c42ef7c-2115-4da9-8a22-bd9c36cdb8b4', '5b60cd25-34e3-4f29-b217-aba2452e89a5'], datelist)

    # #TRANSPARENT FLOORS
    # name = 'transparent_floors'
    # datelist = pd.date_range(end=dt.datetime.today().date(), periods=30)
    # #datelist = pd.date_range(start=dt.date(2025, 8, 6), end=dt.date(2025, 9, 1))
    # test_domains = [
    #     'pro-football-reference.com',
    #     'baseball-reference.com',
    #     'deepai.org',
    #     'signupgenius.com',
    #     'worldofsolitaire.com',
    #     'deckshop.pro',
    #     'adsbexchange.com'
    # ]
    # # enforce = {'dont_enfore': ['adsbexchange.com', 'baseball-reference.com', 'deepai.org'],
    # #            'enforce': ['deckshop.pro', 'pro-football-reference.com', 'signupgenius.com', 'worldofsolitaire.com']}

    # # #TRANSPARENT FLOORS larger test enforced
    # name = 'transparent_floors_sept_16_enforced'
    # datelist = pd.date_range(start=dt.date(2025,9,16), end=dt.datetime.today().date() - dt.timedelta(days=1))
    # test_domains = get_domains_from_collection_ids(['38622a20-b851-40d0-8c4a-ab2ab881fb0a'], datelist) # enforced

    #TRANSPARENT FLOORS larger test not enforced
    name = 'transparent_floors_sept_16_not_enforced'
    datelist = pd.date_range(start=dt.date(2025,9,16), end=dt.datetime.today().date() - dt.timedelta(days=1))
    test_domains = get_domains_from_collection_ids(['b2df7b52-27dc-409d-9876-0d945bad6f6e'], datelist) # not enforced

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

    main_process_csv(tablename, name)
def main_data_explore():
    query_filename = 'query_BI_AB_test_page_hits_data_explore'

    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    df = get_bq_data(query)
    df.transpose().to_csv('AB_data_6.csv')

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
        return df_summary_mean_with_dates.transpose()

    df_summary_std = df[['domain'] + val_cols].groupby(['domain']).agg('std')

    summary_mean_error_dict = {}
    for d in df_summary_mean.index:
        summary_mean_error_dict[d] = df_summary_std.loc[d][val_cols] / np.sqrt(df_summary_mean_with_dates.loc[d]['date_count'].astype('float64') - 1)
    df_summary_mean_error = pd.DataFrame(summary_mean_error_dict).transpose()
    df_summary_mean_error_with_dates = add_date_cols(df_summary_mean_error, df, val_cols)

    df_summary_t_stats = df_summary_mean / df_summary_mean_error
    df_summary_t_stats_with_dates = add_date_cols(df_summary_t_stats, df, val_cols)

    return df_summary_mean_with_dates.transpose(), df_summary_mean_error_with_dates.transpose(), df_summary_t_stats_with_dates.transpose()

def format_worksheet(writer, sheetname, df, cell_format_number_str='0%', max_color_lowest_value=None, max_color_highest_value=None):

    df.to_excel(writer, sheet_name=sheetname)

    worksheet = writer.sheets[sheetname]

    if cell_format_number_str is None:
        worksheet.autofit()
        return

    if (max_color_lowest_value is None) or (max_color_highest_value is None):
        max = abs(df.iloc[3:, ]).max().max()
        max_color_lowest_value = -max
        max_color_highest_value = max

    workbook = writer.book

    cell_range = f'B5:{chr(66 + len(df.columns) - 1)}{len(df) + 1}'

    V = 5
    v = np.append(np.arange(-V, 0), np.arange(1, V+1))
    boundaries = np.arange(len(v)+1) / len(v) * (max_color_highest_value - max_color_lowest_value) + max_color_lowest_value
    boundaries[0] = -1e6
    boundaries[-1] = 1e6
    for i, vi in enumerate(v):
        #print(f'{int(abs(vi))}, {boundaries[i]} : {boundaries[i+1]}')
        cell_format = workbook.add_format()
        cell_format.set_bg_color(Color.theme(5 if vi < 0 else 6, int(abs(vi))))
        cell_format.set_locked(False)
        worksheet.conditional_format(
            cell_range,
            {
                "type": "cell",
                "criteria": "between",
                "minimum": boundaries[i],
                "maximum": boundaries[i+1],
                "format": cell_format,
            },
        )

    cell_format_number = workbook.add_format()
    cell_format_number.set_num_format(cell_format_number_str)
    cell_format_number.set_locked(False)
    worksheet.conditional_format(cell_range, {'type': 'no_errors', 'format': cell_format_number})

    worksheet.autofit()


def main_process_csv(tablename_results, filename_out_xlsx):
    query_filename = 'query_get_AB_test_results_for_csv'
    repl_dict = {'tablename': tablename_results}
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    df_raw = get_bq_data(query, repl_dict)

    index_cols = ['domain', 'date', 'test_name']
    val_cols = [c for c in df_raw.columns if c not in index_cols + ['test_group']]
    df = df_raw.pivot(index=index_cols, columns=['test_group'], values=val_cols).reset_index()

    df_uplift = df[index_cols].copy()
    df_uplift.columns = pd.Index(['domain', 'date', 'test_name'], dtype='object')
    for c in val_cols:
        df_uplift[c] = (df[c][1].astype('float64') - df[c][0].astype('float64')) / (0.5*(df[c][1].astype('float64') + df[c][0].astype('float64')))
    df_uplift = df_uplift.fillna(0)

    summary_mean = create_table_summary(df, val_cols)
    summary_uplift_mean, summary_uplift_error, summary_uplift_t_stats = create_table_summary(df_uplift, val_cols, True)

    writer = pd.ExcelWriter(f'results/{filename_out_xlsx}_AB_test_results.xlsx', engine='xlsxwriter')
    format_worksheet(writer, 'Summary of daily average change', summary_uplift_mean)
    # format_worksheet(writer, 'summary_uplift_error', summary_uplift_error)
    # format_worksheet(writer, 'summary_uplift_t_stats', summary_uplift_t_stats,'#,##0')
    format_worksheet(writer, 'Summary of daily average values', summary_mean, None)
    format_worksheet(writer, 'Raw data', df, None)
    writer.close()


if __name__ == "__main__":

    main()

    # main_process_csv()

    #main_data_explore()