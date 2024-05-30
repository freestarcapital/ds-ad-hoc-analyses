import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)
# Replace with your own values
project_id = "freestar-prod"

client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace(f"<{k}>", v)
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main():
    bins = 200

    rep_dict = {"START_TIMESTAMP_STR": "2024-5-10 10:00:00 UTC",
                "END_TIMESTAMP_STR": "2024-5-11 10:00:00 UTC",
                "DEMAND_PARTNER": "",
                "SELECT_COLS": "prop_of_winning_bid_demand_partner"}

    query = open(os.path.join(sys.path[0], "demand_partner_value_get_demand_partners.sql"), "r").read()
    demand_partners = get_bq_data(query, rep_dict)['bidder_id'].to_list()

    query = open(os.path.join(sys.path[0], "demand_partner_value_total_auctions.sql"), "r").read()
    total_auction_count = get_bq_data(query, rep_dict).iloc[0, 0]

    fig, ax = plt.subplots(figsize=(12, 9))
    for i, dp in enumerate(demand_partners):
        rep_dict["DEMAND_PARTNER"] = dp
        print(f"doing: {dp}")

        df1 = get_bq_data(open(os.path.join(sys.path[0], "demand_partner_value_method1.sql"), "r").read(), rep_dict)
        df2 = get_bq_data(open(os.path.join(sys.path[0], "demand_partner_value_method2.sql"), "r").read(), rep_dict)

        col_name = f"{dp}: incl: {len(df2) / total_auction_count*100:0.0f}%, cpm uplift: {df1['avg_raw_cpm_winner_demand_partner_included_uplift'][0]:0.2f}"

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
    ax.set_xlabel('Ratio of demand partner bid to winning bid (0 = dp included but no bid returned, 1 = won)')
    ax.set_ylabel('Percentage of demand partner bid requests returning bid ratio (or higher)')
    fig.suptitle(f'Demand partner bid ratios for Server Side auction requests made {rep_dict["START_TIMESTAMP_STR"]} to {rep_dict["END_TIMESTAMP_STR"]}')
    fig.savefig('plots/demand_partners.png')

if __name__ == "__main__":
    main()
