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
        query = query.replace(f"<{k}>", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main_bidsresponse_analysis(dt_end, data_hours=1, force_recalc=False):
    bins = 200

    rep_dict = {"DEMAND_PARTNER": "",
    "SELECT_COLS": "prop_of_winning_bid_demand_partner",
    "START_UNIX_TIME_MS": str(int(datetime.datetime.timestamp(dt_end - datetime.timedelta(hours=data_hours)) * 1000)),
    "END_UNIX_TIME_MS": str(int(datetime.datetime.timestamp(dt_end) * 1000))}

    data_cache_filename = f'data_cache/bidsresponse_common_data_{rep_dict['START_UNIX_TIME_MS']}_{rep_dict['END_UNIX_TIME_MS']}'
    if force_recalc or not os.path.exists(data_cache_filename):
        query = open(os.path.join(sys.path[0], "bidsresponse_get_demand_partners.sql"), "r").read()
        demand_partners = get_bq_data(query, rep_dict)['bidder_code'].to_list()
        with open(data_cache_filename, 'wb') as f:
            pickle.dump((demand_partners), f)

    with open(data_cache_filename, 'rb') as f:
        (demand_partners) = pickle.load(f)

    fig, ax = plt.subplots(figsize=(12, 9))
    for i, dp in enumerate(demand_partners):
        data_cache_filename = f'data_cache/bidsresponse_{dp}_{rep_dict['START_UNIX_TIME_MS']}_{rep_dict['END_UNIX_TIME_MS']}.pkl'
        if force_recalc or not os.path.exists(data_cache_filename):
            now = datetime.datetime.now()
            rep_dict["DEMAND_PARTNER"] = dp
            print(f"doing: {dp}, {now}")

            df = get_bq_data(open(os.path.join(sys.path[0], "bidsresponse_query.sql"), "r").read(), rep_dict)
            with open(data_cache_filename, 'wb') as f:
                pickle.dump((df), f)

        with open(data_cache_filename, 'rb') as f:
            (df) = pickle.load(f)

        col_name = dp
        if i == 0:
            y, x, _ = plt.hist(df[rep_dict['SELECT_COLS']], bins=bins, density=True, cumulative=True)
            df_hist = pd.DataFrame(y, x[:-1], columns=[col_name])
        else:
            y, _, _ = plt.hist(df[rep_dict['SELECT_COLS']], bins=x, density=True, cumulative=True)
            df_hist[col_name] = y

    ax.clear()
    df2 = 100*(1 - df_hist)
    col_order = df2.iloc[0, :].sort_values(ascending=False).index.to_list()
    df2 = df2[col_order]
    df2.plot(ax=ax)
    ax.set_xlabel('Ratio of demand partner bid to highest bid (0 = dp not included or no bid, 1 = highest)')
    ax.set_ylabel('Percentage of demand partner bid requests returning at least bid ratio shown on x-axis')
    fig.suptitle(f'Demand partner bid ratios for highest bid in Prebid auctions')
    fig.savefig(f'plots/demand_partners_highest_bid_{dt_end.strftime("%Y%m%d-%H%M")}_{data_hours}.png')

def main_bidswon_analysis(dt_end, data_hours=1, force_recalc=False):
    bins = 200

    rep_dict = {"DEMAND_PARTNER": "",
                "SELECT_COLS": "prop_of_winning_bid_demand_partner",
                "START_UNIX_TIME_MS": str(int(datetime.datetime.timestamp(dt_end - datetime.timedelta(hours=data_hours)) * 1000)),
                "END_UNIX_TIME_MS": str(int(datetime.datetime.timestamp(dt_end) * 1000))}

    data_cache_filename = f'data_cache/bidsresponse_common_data_{rep_dict['START_UNIX_TIME_MS']}_{rep_dict['END_UNIX_TIME_MS']}.pkl'
    print(f'START_UNIX_TIME_MS: {rep_dict["START_UNIX_TIME_MS"]}, END_UNIX_TIME_MS: {rep_dict["END_UNIX_TIME_MS"]}')

    if force_recalc or not os.path.exists(data_cache_filename):
        query = open(os.path.join(sys.path[0], "bidsresponse_get_demand_partners.sql"), "r").read()
        demand_partners = get_bq_data(query, rep_dict)['bidder_code'].to_list()
        with open(data_cache_filename, 'wb') as f:
            pickle.dump((demand_partners), f)

    with open(data_cache_filename, 'rb') as f:
        (demand_partners) = pickle.load(f)

    fig, ax = plt.subplots(figsize=(12, 9))
    for i, dp in enumerate(demand_partners):
        data_cache_filename = f'data_cache/bidswon_{dp}_{rep_dict['START_UNIX_TIME_MS']}_{rep_dict['END_UNIX_TIME_MS']}.pkl'
        if force_recalc or not os.path.exists(data_cache_filename):
            now = datetime.datetime.now()
            rep_dict["DEMAND_PARTNER"] = dp
            print(f"doing: {dp}, {now}")

            df = get_bq_data(open(os.path.join(sys.path[0], "bidswon_query.sql"), "r").read(), rep_dict)
            with open(data_cache_filename, 'wb') as f:
                pickle.dump((df), f)

        with open(data_cache_filename, 'rb') as f:
            (df) = pickle.load(f)

        df = df[df <= 1]

        col_name = dp
        if i == 0:
            y, x, _ = plt.hist(df[rep_dict['SELECT_COLS']], bins=bins, density=True, cumulative=True)
            df_hist = pd.DataFrame(y, x[:-1], columns=[col_name])
        else:
            y, _, _ = plt.hist(df[rep_dict['SELECT_COLS']], bins=x, density=True, cumulative=True)
            df_hist[col_name] = y

    ax.clear()
    df2 = 100 * (1 - df_hist)
    col_order = df2.iloc[0, :].sort_values(ascending=False).index.to_list()
    df2 = df2[col_order]
    df2.plot(ax=ax)
    ax.set_xlabel('Ratio of demand partner bid to highest bid (0 = dp not included or no bid, 1 = winner where prebid won)')
    ax.set_ylabel('Percentage of demand partner bid requests returning at least bid ratio shown on x-axis')
    plot_filename = f'plots/demand_partners_winning_bid_{dt_end.strftime("%Y%m%d-%H%M")}_{data_hours}'
    title = 'Demand partner bid ratios for winning bid where Prebid won the auction'
    fig.suptitle(title)
    fig.savefig(plot_filename + '.png')


if __name__ == "__main__":
    force_recalc = True
    data_hours = 1

    dt_end = datetime.datetime(2024, 6, 30, 10, 0, 0)

    main_bidswon_analysis(dt_end, data_hours=data_hours, force_recalc=force_recalc)

    main_bidsresponse_analysis(dt_end, data_hours=data_hours, force_recalc=force_recalc)
