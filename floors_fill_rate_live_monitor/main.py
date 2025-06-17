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

project_id = "freestar-157323"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()


def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main():


    query = open(os.path.join(sys.path[0], f"query_get_live_data.sql"), "r").read()
    df = get_bq_data(query)

    df_p = df.pivot(index='date_hour', columns='placement_id', values='avg_floor_price')

    fig, ax = plt.subplots(figsize=(12, 9))
    df_p.plot(ax=ax)
    fig.savefig(f'plot.png')

def main_base_data():

    ad_units = [
    '/15184186/aljazeera_incontent_5',
    '/15184186/netronline_pubrecs_728x90_atf_desktop_header_1',
    '/15184186/tagged_160x600_300x250_320x50_320x100_right']#,
    #'/6254/flightawarehttps/flightaware_live',
    #'/6254/flightawarehttps/flightaware_live_airport_leaderboard_atf']

    fig, ax = plt.subplots(figsize=(16, 12), nrows=len(ad_units))
    if len(ad_units) == 1:
        ax = [ax]

    and_where = ""
    #and_where = "and country_code = 'US' and device_category = 'Desktop'"

    a_w_s = and_where.split("'")
    title_extra = f"{len(ad_units)}"
    if len(a_w_s) >= 4:
        title_extra += ' ' + a_w_s[1] + ' ' + a_w_s[3]

    for i, ad_unit in enumerate(ad_units):

        print(f'doing: {ad_unit}')

        ad_unit_domain_base_like = ad_unit.split('_')[0] + '_%'

        query = open(os.path.join(sys.path[0], f"query_get_performance_data_from_base_data.sql"), "r").read()
        df = get_bq_data(query, replacement_dict={'ad_unit': ad_unit,
                                                  'ad_unit_domain_base_like': ad_unit_domain_base_like,
                                                  'and_where': and_where})

        #df = df[['date', 'floor_price_fr', 'fill_rate_fr']]
        df_p = df.set_index('date').astype('float64')
        df_p.plot(ax=ax[i], title=f'{ad_unit}{title_extra}', secondary_y=['cpma_rm', 'cpma_fr'])

    fig.savefig(f'plot_results_{title_extra.replace(' ', '_')}.png')


    g = 0

if __name__ == "__main__":
    #main()
    main_base_data()

