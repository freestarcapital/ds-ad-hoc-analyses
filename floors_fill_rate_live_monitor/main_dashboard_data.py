import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
from matplotlib.backends.backend_pdf import PdfPages

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

    query = open(os.path.join(sys.path[0], f"query_get_performance_data_from_base_data_for_dashboard.sql"), "r").read()
    query_reference_ad_units = open(os.path.join(sys.path[0], f"query_get_reference_ad_units.sql"), "r").read()

    ad_units = pd.read_csv('fill-rate-ads.csv')

    and_where = ""
    #and_where = "and country_code = 'US' and device_category = 'Desktop'"

    a_w_s = and_where.split("'")
    title_extra = f"{len(ad_units)}"
    if len(a_w_s) >= 4:
        title_extra += ' ' + a_w_s[1] + ' ' + a_w_s[3]

    with PdfPages(f'plots/fill-rate_results_{title_extra.replace(' ', '_')}.pdf') as pdf:

        for _, (ad_unit, domain) in ad_units.iterrows():

            if (',' in ad_unit) or ('test' in ad_unit):
                continue

            print(f"ad_unit: {ad_unit}")

            reference_ad_units_where = f"ad_unit_name like '{ad_unit.split('_')[0]}\\\\_%'"
            for ad_unit_other in ad_units[ad_units['domain'] == domain]['ad_unit']:
                reference_ad_units_where += f" and ad_unit_name != '{ad_unit_other}'"

            repl_dict = {'ad_unit': ad_unit,
                         'reference_ad_units_where': reference_ad_units_where,
                         'and_where': and_where}

            df_reference_ad_units = get_bq_data(query_reference_ad_units, repl_dict)
            print (df_reference_ad_units)

            df = get_bq_data(query, repl_dict)
            if not df.empty:
                df_p = df.set_index('date').astype('float64')
                plot_cols = ['floor_price', 'fill_rate', 'cpma']
                fig, ax = plt.subplots(figsize=(16, 12), nrows=len(plot_cols))
                for i, col in enumerate(plot_cols):
                    df_p[[c for c in df_p.columns if col in c]].plot(ax=ax[i])
                fig.suptitle(f'{ad_unit}{title_extra}, number of reference ad_units: {len(df_reference_ad_units)}')
                pdf.savefig()




    fig, ax = plt.subplots(figsize=(12, 9))
    df_p.plot(ax=ax)
    fig.savefig(f'plot.png')



if __name__ == "__main__":
    main()
