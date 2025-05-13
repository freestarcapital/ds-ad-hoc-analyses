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

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{"+k+"}", f'{v}')
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def get_data(query_filename, data_cache_filename=None, force_requery=False, repl_dict = {}):
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


def get_cdf(x, col_name):
    return pd.DataFrame(np.arange(len(x)) / len(x), index=pd.Index(x[col_name].sort_values()), columns=[col_name])

def do_scatter(x, x_col, y_col, ax):
    ax.scatter(x[x_col], x[y_col])
    reg = LinearRegression(fit_intercept=False).fit(x[[x_col]], x[y_col])
    x_max = x[x_col].max()
    ax.plot([0, x_max], [0, x_max * reg.coef_[0]], c='r')
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    r2 = reg.score(x[[x_col]], x[y_col])
    title_text = f'{y_col} ~ {reg.coef_[0]:0.2f} x {x_col}, R^2: {100 * r2:0.1f}%'
    ax.set_title(title_text)

def main():
    x = get_data('get_expt_data_floor_price', force_requery=False)

    fig, ax = plt.subplots(figsize=(16, 12), nrows=2, ncols=2)
    for col_name in x.columns:
        get_cdf(x, col_name).plot(ax=ax[0, 0])
    ax[0, 0].set_xlim([0, 2])

    do_scatter(x, 'floor_price_no_user', 'floor_price_user', ax[0, 1])
    do_scatter(x, 'floor_price_prod', 'floor_price_no_user', ax[1, 0])
    do_scatter(x, 'floor_price_prod', 'floor_price_user', ax[1, 1])

    fig.savefig('cdf.png')

    h = 0

if __name__ == "__main__":

    main()
