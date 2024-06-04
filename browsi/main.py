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

project_id = "freestar-157323"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace(f"<{k}>", v)
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main_browsi_1(force_recalc=False):

    bins = 200

    data_cache_filename = f'data_cache/browsi_1.pkl'
    if force_recalc or not os.path.exists(data_cache_filename):
        query = open(os.path.join(sys.path[0], "browsi_query_1.sql"), "r").read()
        df = get_bq_data(query)
        with open(data_cache_filename, 'wb') as f:
            pickle.dump(df, f)

    with open(data_cache_filename, 'rb') as f:
        df = pickle.load(f)

    df['price_prediction'] = [float(x) for x in df['price_prediction'].values]

    df2 = df[(df['cpma'] < 2) & (df['price_prediction'] < 0.4)].copy()
    df2['log_cpma'] = np.log(df2['cpma']+0.1)

    for cpma_type in ['cpma', 'log_cpma']:
        res = stats.linregress(df2['price_prediction'], df2[cpma_type])
        title = f'{cpma_type} = {res.intercept:0.2f} + {res.slope:0.2f} * browsi price_prediction, R^2: {100 * res.rvalue ** 2:0.1f}%'
        fig = px.density_heatmap(df2, x='price_prediction', y=cpma_type, nbinsx=50, nbinsy=100, title=title)
        fig.write_image(f"plots/browsi_{cpma_type}.png")

    N = 5
    plot_specs = [('cpma', False, 2), ('cpma', True, 2), ('rpp', True, 80)]
    fig, ax = plt.subplots(figsize=(16, 12), nrows=len(plot_specs))
    for ax_i, (col_to_plot, cumulative, x_max) in enumerate(plot_specs):
        ax_ = ax[ax_i]
        col_stats = {}

        price_prediction_lower = -0.001
        for i, n in enumerate(np.arange(1, N+1)):
            price_prediction_upper = df2['price_prediction'].quantile(n / N)
            col_name = f'{price_prediction_lower:0.2f} to {price_prediction_upper:0.2f}'

            df3 = df2[(price_prediction_lower < df2['price_prediction']) & (df2['price_prediction'] <= price_prediction_upper)]
            col_stats[col_name] = {"mean": df3[col_to_plot].mean(), "var": df3[col_to_plot].mean()}

            if i == 0:
                y, x, _ = plt.hist(df3[col_to_plot], bins=bins, density=True, cumulative=cumulative)
                df_hist = pd.DataFrame(y, x[:-1], columns=[col_name])
            else:
                y, _, _ = plt.hist(df3[col_to_plot], bins=x, density=True, cumulative=cumulative)
                df_hist[col_name] = y

            price_prediction_lower = price_prediction_upper

        buckets = list(col_stats.keys())
        z_scores = np.zeros([N, N])
        p_scores = np.zeros([N, N])
        for n1 in range(N):
            for n2 in range(N):
                z_scores[n1, n2] = (col_stats[buckets[n1]]['mean'] - col_stats[buckets[n2]]['mean']) / np.sqrt(
                    col_stats[buckets[n1]]['var'] + col_stats[buckets[n2]]['var'])
                p_scores[n1, n2] = 1 - stats.norm.sf(
                    abs(col_stats[buckets[n1]]['mean'] - col_stats[buckets[n2]]['mean']) / np.sqrt(
                        col_stats[buckets[n1]]['var'] + col_stats[buckets[n2]]['var']))

        col_rename = {}
        for col_i, col in enumerate(df_hist.columns):
            col_leg = f'{col}: '

            if cumulative:
                for cum_val in [0.1, 0.25, 0.5, 0.75, 0.9]:
                    x_val = abs(df_hist[col] - cum_val).idxmin()
                    col_leg += f'{100 * cum_val:0.0f}% <{x_val:0.2f}, '
            col_leg += f'mean: {col_stats[col]["mean"]:0.2f}'
            if not cumulative:
                p_scores_str = ", ".join([f'{100 * x:0.0f}%' for x in p_scores[col_i, :]])
                col_leg += f', std: {np.sqrt(col_stats[col]["var"]):0.2f}, p-scores: {p_scores_str}'

            col_rename[col] = col_leg
        df_hist = df_hist.rename(columns=col_rename)

        ax_.clear()
        df_hist.plot(ax=ax_, xlim=[0, x_max])
        ax_.set_xlabel(f'{col_to_plot}')
        ax_.set_ylabel(f'{"CDF" if cumulative else "PDF"} of sess with avg {col_to_plot} <= x-axis val')
        fig.suptitle(f'Does browsi signal prediction session value? browsi data split into {N} equal bins. (auction_end)')

    fig.savefig('plots/browsi_density_plots.png')


    j = 9

if __name__ == "__main__":


    main_browsi_1()