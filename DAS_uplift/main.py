import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime
import pickle
import plotly.express as px
import kaleido
from scipy.stats import linregress

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
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main():
    rep_dict = {'days_back_start': 8,
                'days_back_end': 1,
                'minimum_session_count': 100}


    dims_list = [
        ["device_category", "country_code", "rtt_category"],
        ["domain"],
        # #       ["ad_product"],
        # ["domain", "device_category"],
        # #              ["ad_product", "device_category"],
        # ["domain", "country_code"],
        # #     ["ad_product", "country_code"],
        ["domain", "device_category", "country_code"],
        # #    ["ad_product",  "device_category", "country_code"],
        ["domain", "device_category", "country_code", "rtt_category", "fsrefresh"]]  # ,
        # #   ["ad_product", "device_category", "country_code", "rtt_category", "fsrefresh"],
        # #   ["domain", "ad_product", "device_category", "country_code", "rtt_category", "fsrefresh"]]

    df_list = []

    for dims in dims_list:
        rep_dict['dims'] = ", ".join(dims)
        print(f'doing: {rep_dict["dims"]}')

        query = open(os.path.join(sys.path[0], f"query_uplift.sql"), "r").read()
        df = get_bq_data(query, rep_dict)

        uplift = df['rps_uplift_ratio_perc'].values

        z = pd.DataFrame(np.arange(0, 100, 100 / len(uplift)), index=pd.Index(uplift), columns=[rep_dict['dims']])
        df_list.append(z)

    df = pd.concat(df_list)
    fig, ax = plt.subplots(figsize=(16, 12))
    df.plot(ax=ax, xlim=[df.index[0], 200], xlabel='optimised vs experiment percentage uplift in rps in each cohort',
            ylabel='cumulative percentage of cohort', title='Cumulative percentage rps uplift bu cohort')
    fig.savefig('plots/uplift.png')

if __name__ == "__main__":
    main()

