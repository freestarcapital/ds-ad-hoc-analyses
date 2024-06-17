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
        query = query.replace(f"<{k}>", v)
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main():
    rep_dict = {"DAYS_BACK_START": "9",
                "DAYS_BACK_END": "2"}

    df_list = []
    df_partial_improvement_list = []
    summary = {}
    for cc in ['_country_merge', '']:

        rep_dict['TABLE_NAME'] = f'das_extra_dim_results_all{cc}_base_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}'
        query = open(os.path.join(sys.path[0], f"base_query{cc}.sql"), "r").read()
        print(f'doing: {cc}, base query')
        df = get_bq_data(query, rep_dict)
        df['cc'] = cc
        df['extra_dim'] = 'None'
        print(df)
        df_list.append(df)

        query = f"select count(*) total_cohorts " \
                f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`"
        df_3 = get_bq_data(query, rep_dict)
        summary[f'total_cohorts_base'] = df_3.iloc[0, 0]

        for extra_dim in ['ad_product', 'domain', 'ad_product, domain']:
            rep_dict['EXTRA_DIM'] = extra_dim
            rep_dict['TABLE_NAME'] = f'das_extra_dim_results_all{cc}_{extra_dim.replace(', ', '')}_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}'

            query = open(os.path.join(sys.path[0], f"extra_dims_query{cc}.sql"), "r").read()
            print(f'doing: {cc}, {extra_dim}')
            df = get_bq_data(query, rep_dict)
            df['cc'] = cc
            df['extra_dim'] = extra_dim
            print(df)
            df_list.append(df)

            query = f"select sum(revenue_domain - revenue_no_domain) over(order by revenue_domain - revenue_no_domain desc) additional_revenue "\
                f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` "\
                f"where revenue_domain - revenue_no_domain > 0 "\
                f"order by revenue_domain - revenue_no_domain desc"
            df_2 = get_bq_data(query, rep_dict)

            query = f"select sum(revenue_no_domain) total_revenue_no_domain, count(*) total_cohorts "\
                f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`"
            df_3 = get_bq_data(query, rep_dict)

            legend_text = f"{extra_dim.replace(', ', '_')}: max added cohorts: {df_3.iloc[0, 1] / 1e3:0.0f}k, max rev uplift: {df_2.iloc[-1, 0] / df_3.iloc[0, 0] * 100:0.1f}%"
#            legend_text = f"{extra_dim.replace(', ', '_')}: max rev uplift: {df_2.iloc[-1, 0] / df_3.iloc[0, 0] * 100:0.1f}%"
            df_2 = df_2.rename(columns={'additional_revenue': legend_text})

            df_partial_improvement_list.append(df_2)
#            df_partial_improvement_list.append(df_2 / df_3.iloc[0, 0] * 100)
            summary[f'total_cohorts_{extra_dim}'] = df_3.iloc[0, 1]

        fig, ax = plt.subplots(figsize=(12, 9))
        df_partial_improvement = pd.concat(df_partial_improvement_list, axis=1)#.ffill()
        title = f"Revenue increase as overrides are added to {summary['total_cohorts_base'] / 1e3:0.0f}k base cohort"
        (df_partial_improvement / 1e6).plot(logx=True, xlabel='Number of overrides', ylabel='Increase in revenue ($m)', title=title, ax=ax)
        fig.savefig(f'plots/override_improvement{cc}.png')

        g = 0

    df_results = pd.concat(df_list)
    df_results.to_csv(f'results_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}.csv')
    print(df_results)



    h = 0

if __name__ == "__main__":
    main()
