
import numpy as np
import xlsxwriter
# from xlsxwriter.color import Color
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery_storage
import os, sys
import datetime as dt
import pickle

bqstorageclient = bigquery_storage.BigQueryReadClient()

# def get_data(query_filename, client, data_cache_filename=None, force_requery=False, repl_dict={}):
#
#     if data_cache_filename is None:
#         data_cache_filename = query_filename
#     data_cache_filename_full = f'data_cache/{data_cache_filename}.pkl'
#
#     if not force_requery and os.path.exists(data_cache_filename_full):
#         print(f'found existing data file, loading {data_cache_filename_full}')
#         with open(data_cache_filename_full, 'rb') as f:
#             df = pickle.load(f)
#         return df
#
#     query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
#     df = get_bq_data(query, client, repl_dict)
#
#     with open(data_cache_filename_full, 'wb') as f:
#         pickle.dump(df, f)
#     return df

def get_bq_data(query, client, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))

    df = client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')
    return df


def add_date_cols(df_summary_in, df, val_cols, group_cols):
    df_summary = df_summary_in.copy()
    for ag in ['min', 'max', 'count']:
        df_summary[f'date_{ag}'] = df[group_cols + ['date']].groupby(group_cols).agg(ag)
    df_summary = df_summary[['date_min', 'date_max', 'date_count'] + val_cols]
    return df_summary

def create_table_summary(df, val_cols, index_cols, calculate_errors_and_t_stats=False):

    group_cols = [c for c in index_cols if c != 'date']

    df_summary_mean = df[group_cols + val_cols].groupby(group_cols).agg('mean')
    df_summary_mean_with_dates = add_date_cols(df_summary_mean, df, val_cols, group_cols)

    if not calculate_errors_and_t_stats:
        return df_summary_mean_with_dates.transpose()

    df_summary_std = df[group_cols + val_cols].groupby(group_cols).agg('std')

    summary_mean_error_dict = {}
    for d in df_summary_mean.index:
        summary_mean_error_dict[d] = df_summary_std.loc[d][val_cols] / np.sqrt(df_summary_mean_with_dates.loc[d]['date_count'].astype('float64') - 1)
    df_summary_mean_error = pd.DataFrame(summary_mean_error_dict).transpose()
    df_summary_mean_error_with_dates = add_date_cols(df_summary_mean_error, df, val_cols, group_cols)

    df_summary_t_stats = df_summary_mean / df_summary_mean_error
    df_summary_t_stats_with_dates = add_date_cols(df_summary_t_stats, df, val_cols, group_cols)

    return df_summary_mean_with_dates.transpose(), df_summary_mean_error_with_dates.transpose(), df_summary_t_stats_with_dates.transpose()

def format_worksheet(writer, sheetname, df, first_row_format, cell_format_number_str='0.0%', max_color_lowest_value=None, max_color_highest_value=None):

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

    cell_range = f'A{first_row_format}:ZZ{len(df) + 1}'

    V = 4
    v = np.append(np.arange(-V, 1), np.arange(0, V+1))
    boundaries = np.arange(len(v)+1) / len(v) * (max_color_highest_value - max_color_lowest_value) + max_color_lowest_value
    boundaries[0] = -1e6
    boundaries[-1] = 1e6
    for i, vi in enumerate(v):
        #print(f'{int(abs(vi))}, {boundaries[i]} : {boundaries[i+1]}')
        if vi == 0:
            continue
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


def main_process_csv(tablename_results, query_filename_in, client):

    if query_filename_in == 'query_FI_AB_test_performance':
        index_cols = ['domain', 'date']
        first_row_format = 5
    elif query_filename_in == 'query_bidder_impact':
        index_cols = ['domain', 'bidder', 'date']
        first_row_format = 7
    else:
        assert False, f'unknown query_filename_in: {query_filename_in}'

    query_filename = f'{query_filename_in}_get_results'
    repl_dict = {'tablename': tablename_results}
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    df_raw = get_bq_data(query, client, repl_dict)

    ab_test_names = df_raw['ab_test_name'].unique()

    for ab_test in ab_test_names:
        print(ab_test)
        df_ab_test = df_raw[df_raw['ab_test_name'] == ab_test]
        df_ab_test = df_ab_test.drop('ab_test_name', axis='columns')

        val_cols = [c for c in df_ab_test.columns if c not in index_cols + ['test_group']]
        for v in val_cols:
            df_ab_test[v] = df_ab_test[v].fillna(0)
        df = df_ab_test.pivot(index=index_cols, columns=['test_group'], values=val_cols).reset_index()

        df_uplift = df[index_cols].copy()
        df_uplift.columns = pd.Index(index_cols, dtype='object')
        for c in val_cols:
            df_uplift[c] = ((df[c][1].astype('float64') - df[c][0].astype('float64')) / (0.5*(df[c][1].astype('float64') + df[c][0].astype('float64')))).fillna(0)

        summary_mean = create_table_summary(df, val_cols, index_cols)
        summary_uplift_mean, summary_uplift_error, summary_uplift_t_stats = create_table_summary(df_uplift, val_cols, index_cols, True)

        writer = pd.ExcelWriter(f'results/{query_filename_in.replace("query_","")}_{ab_test}_{dt.datetime.today().strftime("%Y%m%d")}.xlsx',
                                engine='xlsxwriter')
        format_worksheet(writer, 'Summary of daily average change', summary_uplift_mean, first_row_format, max_color_lowest_value=-1, max_color_highest_value=1)
        # format_worksheet(writer, 'summary_uplift_error', summary_uplift_error, first_row_format)
        # format_worksheet(writer, 'summary_uplift_t_stats', summary_uplift_t_stats, first_row_format, '#,##0')
        format_worksheet(writer, 'Summary of daily average values', summary_mean, first_row_format, None)
        format_worksheet(writer, 'Raw data', df, first_row_format, None)
        writer.close()
