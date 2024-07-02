import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime
import pickle

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

project_id = "freestar-prod"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace(f"<{k}>", v)
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main(force_recalc=False):
    bins = 200

    rep_dict = {"START_TIMESTAMP_STR": "2024-6-29 10:00:00 UTC",
                "END_TIMESTAMP_STR": "2024-6-30 10:00:00 UTC",
                "DEMAND_PARTNER": "",
                "SELECT_COLS": "prop_of_winning_bid_demand_partner"}

    data_cache_filename = f'data_cache/common_data_{rep_dict['START_TIMESTAMP_STR']}_{rep_dict['END_TIMESTAMP_STR']}.pkl'
    if force_recalc or not os.path.exists(data_cache_filename):
        query = open(os.path.join(sys.path[0], "demand_partner_value_get_demand_partners.sql"), "r").read()
        demand_partners = get_bq_data(query, rep_dict)['bidder_id'].to_list()
        query = open(os.path.join(sys.path[0], "demand_partner_value_total_auctions.sql"), "r").read()
        total_auction_count = get_bq_data(query, rep_dict).iloc[0, 0]
        with open(data_cache_filename, 'wb') as f:
            pickle.dump((demand_partners, total_auction_count), f)

    with open(data_cache_filename, 'rb') as f:
        (demand_partners, total_auction_count) = pickle.load(f)

    total = 0
    fig, ax = plt.subplots(figsize=(12, 9))
    for i, dp in enumerate(demand_partners):
        data_cache_filename = f'data_cache/{dp}_{rep_dict['START_TIMESTAMP_STR']}_{rep_dict['END_TIMESTAMP_STR']}.pkl'
        if force_recalc or not os.path.exists(data_cache_filename):
            now = datetime.datetime.now()
            rep_dict["DEMAND_PARTNER"] = dp
            print(f"doing: {dp}, {now}")

            df1 = get_bq_data(open(os.path.join(sys.path[0], "demand_partner_value_method1.sql"), "r").read(), rep_dict)
            df2 = get_bq_data(open(os.path.join(sys.path[0], "demand_partner_value_method2.sql"), "r").read(), rep_dict)
            with open(data_cache_filename, 'wb') as f:
                pickle.dump((df1, df2), f)

        with open(data_cache_filename, 'rb') as f:
            (df1, df2) = pickle.load(f)

        incl = len(df2) / total_auction_count
        cpm_uplift = df1['avg_raw_cpm_winner_demand_partner_included_uplift'][0]
        cpm_uplift_bid = df1['avg_raw_cpm_winner_demand_partner_bid_uplift'][0]
        winning_prop = (df2.values == 1).sum() / len(df2)

        total += winning_prop * incl
        print(f'{dp}: incl: {incl:0.2f}, win prop: {winning_prop:0.2f}, total: {total:0.4f}')

        col_name = (f"{dp}: incl: {incl*100:0.0f}%, cpm uplift: {cpm_uplift:0.2f}, "
                    f"cpm uplift bid: {cpm_uplift_bid:0.2f}")

        if i == 0:
            y, x, _ = plt.hist(df2[rep_dict['SELECT_COLS']], bins=bins, density=True, cumulative=True)
            df_hist = pd.DataFrame(y, x[:-1], columns=[col_name])
        else:
            y, _, _ = plt.hist(df2[rep_dict['SELECT_COLS']], bins=x, density=True, cumulative=True)
            df_hist[col_name] = y

    ax.clear()
    df2 = 100*(1 - df_hist)
    col_order = df2.iloc[0, :].sort_values(ascending=False).index.to_list()
    df2 = df2[col_order]
    df2.plot(ax=ax)
    ax.set_xlabel('Ratio of demand partner bid to winning bid (0 = dp included but no bid returned, 1 = highest bid of server auction)')
    ax.set_ylabel('Percentage of demand partner bid requests returning bid ratio (or higher)')
    fig.suptitle(f'Demand partner bid ratios for Server Side auction requests made {rep_dict["START_TIMESTAMP_STR"]} to {rep_dict["END_TIMESTAMP_STR"]} {total:0.4f}')
    fig.savefig('plots/demand_partners.png')

if __name__ == "__main__":
    main()
