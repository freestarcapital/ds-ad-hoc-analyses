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


def main_create_ab_test_data_table():

    repl_dict = {'start_date': '2024-08-19',
                 'end_date': '2024-08-26'}

    query = open(os.path.join(sys.path[0], f'query_all_bidders_on_DAS_ab_test.sql'), "r").read()
    get_bq_data(query, repl_dict)


def main():
    # for country_code there are about 200 countries. Bonferroni says we should divide our confidence 15% by 200,
    # but let's be a lot less conservative and go for divide by 10, giving 1.5%, which is about 2.5 standard deviations

    # for domain there are about 100 domain. Bonferroni says we should divide our confidence 15% by 100,
    # but let's be a lot less conservative and go for divide by 6, giving 2.5%, which is about 2 standard deviations

    std_significance = {'country_code': 2.5,
                        'domain': 2}

    uncertainty_numerator = 4

    repl_dict = {'start_date': '2024-08-20',
                 'end_date': '2024-08-23'}

    query = open(os.path.join(sys.path[0], f'query_all_bidders_on_DAS_ab_test_results.sql'), "r").read()

    for domain_or_country_code in ['domain', 'country_code']:
        repl_dict['domain_or_country_code'] = domain_or_country_code

        df = get_bq_data(query, repl_dict)

        df['rps_1_uplift_prop'] = (df['rps_1'] - df['rps_0']) / (0.5 * ((df['rps_1'] + df['rps_0'])))

        df['rps_0_uncertainy_prop'] = uncertainty_numerator / np.sqrt(df['sessions_0'])
        df['rps_1_uncertainy_prop'] = uncertainty_numerator / np.sqrt(df['sessions_1'])

        df['rps_diff_uncertainy_prop'] = np.sqrt(df['rps_0_uncertainy_prop'] ** 2 + df['rps_1_uncertainy_prop'] ** 2)
        df['rps_1_uplift_over_uncertainty'] = df['rps_1_uplift_prop'] / df['rps_diff_uncertainy_prop']
        df['statistically_significant'] = abs(df['rps_1_uplift_over_uncertainty']) > std_significance[domain_or_country_code]

        df.to_csv(f'plots/DAS_all_bidders_on_ab_test_{domain_or_country_code}.csv')

    g = 0


if __name__ == "__main__":
    #main_create_ab_test_data_table()
    main()
