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
        query = query.replace(f"<{k}>", v)
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def plot_scatter(df, title, filename, x_col='rps_domain', y_col='rps_domain_opt'):
    x = df[x_col] * 1000
    y = df[y_col] * 1000
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(x, y)
    result = linregress(x, y)
    X = np.array([min(x), max(x)])
    plt.plot(X, result.intercept + result.slope * X, 'r-')
    plt.title(
        f'{title}: {y_col} ~ {result.intercept:0.2f} + {result.slope:0.2f} x {x_col}, R^2: {100 * (result.rvalue ** 2):0.1f}%')
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    fig.savefig(f'plots/{filename}.png')

def plot_percentage_variation(df, extra_dim, filename):
    z = df['percent_diff']
    y, x, _ = plt.hist(z, bins=200, density=True, cumulative=False)
    fig, ax = plt.subplots(figsize=(12, 9))
    pd.DataFrame(y, index=pd.Index(x[1:])).plot(xlim=[-50, 50], legend=None, ax=ax)
    ax.set_xlabel('Percentage increase in optimised dataset revenue vs experimental dataset revenue')
    ax.set_ylabel('Density function')
    fig.suptitle(f'Histogram of optimised revenue vs experimental revenue for {extra_dim}, mean: {np.mean(z):0.1f}%, stddev: {np.std(z):0.1f}%')
    fig.savefig(f'plots/{filename}_base.png')


def main():
    s100 = '_100'
    #s100 = ''

    max_client_bidders = 8
    max_total_bidders = 13

    plots_only = False

    rep_dict = {"DAYS_BACK_START": "9",
                "DAYS_BACK_END": "2"}

    df_list = []
    for cc in ['_no_country', '_country_merge']:#, '']:
        df_partial_improvement_list = []
        summary = {}

        rep_dict['TABLE_NAME'] = f'das_extra_dim_results_all{cc}_base_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}'
        if not plots_only:
            query = open(os.path.join(sys.path[0], f"base_query{cc}.sql"), "r").read()
            print(f'doing: {cc}, base query')
            df = get_bq_data(query, rep_dict)
            df['cc'] = cc
            df['extra_dim'] = 'None'
            print(df)
            df_list.append(df)

        query = f"select rps, rps_opt from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` where rps_opt is not NULL"
        plot_scatter(get_bq_data(query, rep_dict), 'base', f'override_improvement_scatter{cc}_base', 'rps', 'rps_opt')

        query = (f"select safe_divide(revenue_opt-revenue, 0.5*(revenue_opt+revenue)) * 100 percent_diff "
                 f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` "
                 f"where revenue_opt is not null")
        plot_percentage_variation(get_bq_data(query, rep_dict), 'base', f'override_improvement_rev_var{cc}_base')

        query = (f"select count(*) total_cohorts "
                 f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`")
        df_3 = get_bq_data(query, rep_dict)
        summary[f'total_cohorts_base'] = df_3.iloc[0, 0]

        query = (f"select sum(if(status = 'client', 1, 0)) client, "
                 f"sum(if(status != 'off', 1, 0)) on_status "
                 f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` "
                 f"group by country_code, device_category, rtt_category, fsrefresh")
        df_5 = get_bq_data(query, rep_dict)

        bins = np.arange(df_5.max().max() + 1)
        status_counts = pd.DataFrame(index=pd.Index(bins[:-1]))
        for col_name, col_data in df_5.items():
            y, _, _ = plt.hist(col_data, bins=bins, density=True, cumulative=True)
            status_counts[col_name] = 100 * y
            N = max_total_bidders if 'on' in col_name else max_client_bidders
            status_counts = status_counts.rename(columns={
                col_name: f'{col_name}, count <= {N}: {status_counts[col_name].loc[N]:0.1f}%, mean count: {np.mean(col_data):0.1f}'})

        fig, ax = plt.subplots(figsize=(12, 9))
        status_counts.plot(ax=ax)
        ax.set_xlabel('Number (count) of bidders with status shown')
        ax.set_ylabel('Percentage of cohorts with at least bidder count shown on x-axis')
        fig.suptitle(f'Client and on bidder count status when no dimensions added')
        fig.savefig(f'plots/override_improvement_bidder_count{cc}_base.png')

        for extra_dim in ['ad_product', 'domain', 'ad_product, domain']:
            rep_dict['EXTRA_DIM'] = extra_dim
            extra_dim_clean = extra_dim.replace(', ', '')
            rep_dict['TABLE_NAME'] = f'das_extra_dim_results_all{cc}_{extra_dim_clean}_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}'

            if not plots_only:
                query = open(os.path.join(sys.path[0], f"extra_dims_query{cc}.sql"), "r").read()
                print(f'doing: {cc}, {extra_dim}')
                df = get_bq_data(query, rep_dict)
                df['cc'] = cc
                df['extra_dim'] = extra_dim
                print(df)
                df_list.append(df)

            query = f"select rps_domain, rps_domain_opt from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` where rps_domain_opt is not NULL"
            plot_scatter(get_bq_data(query, rep_dict), extra_dim_clean, f'override_improvement_scatter{cc}_{extra_dim_clean}')

            query = (f"select safe_divide(revenue_domain_opt_rps-revenue_domain, 0.5*(revenue_domain_opt_rps+revenue_domain)) * 100 percent_diff "
                     f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` "
                     f"where revenue_domain_opt_rps is not null")
            plot_percentage_variation(get_bq_data(query, rep_dict), extra_dim_clean, f'override_improvement_rev_var{cc}_{extra_dim_clean}')

            query = (f"select sum(revenue_domain{s100} - revenue_no_domain) over(order by revenue_domain{s100} - revenue_no_domain desc) additional_revenue "
                     f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` "
                     f"where status_domain{s100} != status_no_domain "
                     f"order by revenue_domain{s100} - revenue_no_domain desc")
            df_2 = get_bq_data(query, rep_dict)
            df_2 = df_2.set_index(pd.Index(1 + np.arange(len(df_2))))

            query = (f"select sum(revenue_no_domain) total_revenue_no_domain "
                     f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`")
            df_3 = get_bq_data(query, rep_dict)

            legend_text = f"{extra_dim.replace(', ', '_')}: max added cohorts: {len(df_2) / 1e3:0.0f}k, max rev uplift: {df_2.iloc[-1, 0] / df_3.iloc[0, 0] * 100:0.1f}%"
            df_2 = df_2.rename(columns={'additional_revenue': legend_text})

            df_partial_improvement_list.append(df_2 / df_3.iloc[0, 0] * 100)
            summary[f'extra_cohorts_{extra_dim}'] = len(df_2)

            query = (f"select sum(if(status_no_domain = 'client', 1, 0)) no_{extra_dim_clean}_client, "
                     f"sum(if(status_domain = 'client', 1, 0)) {extra_dim_clean}_client, "
                     f"sum(if(status_no_domain != 'off', 1, 0)) no_{extra_dim_clean}_on, "
                     f"sum(if(status_domain != 'off', 1, 0)) {extra_dim_clean}_on "
                     f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}` "
                     f"group by country_code, device_category, rtt_category, fsrefresh, {extra_dim}")
            df_5 = get_bq_data(query, rep_dict)

            bins = np.arange(df_5.max().max()+1)
            status_counts = pd.DataFrame(index=pd.Index(bins[:-1]))
            for col_name, col_data in df_5.items():
                y, _, _ = plt.hist(col_data, bins=bins, density=True, cumulative=True)
                status_counts[col_name] = 100 * y
                N = max_total_bidders if 'on' in col_name else max_client_bidders
                status_counts = status_counts.rename(columns={col_name: f'{col_name}, count <= {N}: {status_counts[col_name].loc[N]:0.1f}%, mean count: {np.mean(col_data):0.1f}'})

            fig, ax = plt.subplots(figsize=(12, 9))
            status_counts.plot(ax=ax)
            ax.set_xlabel('Number (count) of bidders with status shown')
            ax.set_ylabel('Percentage of cohorts with at least bidder count shown on x-axis')
            fig.suptitle(f'Effect of adding dimension {extra_dim} to client and on bidder count status')
            fig.savefig(f'plots/override_improvement_bidder_count{cc}_{extra_dim_clean}.png')

        fig, ax = plt.subplots(figsize=(12, 9), nrows=2)
        df_partial_improvement = pd.concat(df_partial_improvement_list, axis=1)
        title = f"Revenue increase as overrides are added to {summary['total_cohorts_base'] / 1e3:0.0f}k base cohorts"
        df_partial_improvement.plot(xlabel='Number of overrides', ylabel='Percent increase in revenue', title=title, ax=ax[0])
        df_partial_improvement.plot(logx=True, xlabel='Number of overrides', ylabel='Percent increase in revenue', title=title, ax=ax[1])
        fig.savefig(f'plots/override_improvement{cc}{s100}.png')

    if not plots_only:
        df_results = pd.concat(df_list)
        df_results.to_csv(f'results_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}.csv')
        print(df_results)


def main_bootstrap_rev():
    do_calc_queries = True

    rep_dict = {"DAYS_BACK_START": "9",
                "DAYS_BACK_END": "2"}

    for cc in ['_country_merge', '']:

        rep_dict['TABLE_NAME'] = f'das_extra_dim_results_all{cc}_base_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}'

        if do_calc_queries:
            query = open(os.path.join(sys.path[0], f"base_query{cc}.sql"), "r").read()
            print(f'doing: {cc}, base query')
            get_bq_data(query, rep_dict)

        query = f"select sum(revenue) from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`"
        base_mean = get_bq_data(query).iloc[0, 0]

        NN = 1000

        hist_df_list = []
        for rand_prop in [0.1, 0.25, 0.5, 0.75]:
            query = "select "
            query += ",".join(np.repeat(f"sum(if(rand()< {rand_prop},coalesce(revenue_opt,revenue),revenue))", NN))
            query += f" from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`"
            revenue_estimates = get_bq_data(query).iloc[0, :].values

            bin_counts, bins, _ = plt.hist(revenue_estimates, bins=100, density=True, cumulative=True)
            hist_df_list.append(
                pd.DataFrame(bin_counts, index=pd.Index(bins[1:]), columns=[f'rand_perc_{rand_prop * 100:0.0f}']))

        revenue_df = pd.concat(hist_df_list).sort_index().bfill().ffill()
        fig, ax = plt.subplots(figsize=(12, 9))
        revenue_df.plot(ax=ax, xlabel='Revenue', ylabel='Cumulative probability of revenue shown on x-axis',
                       title=f'Revenue estimate with uncertainty for base, revenue:{base_mean:0.0f}')
        fig.savefig(f'plots/revenue_variation_base{cc}.png')

        for extra_dim in ['ad_product', 'domain', 'ad_product, domain']:
            rep_dict['EXTRA_DIM'] = extra_dim
            extra_dim_clean = extra_dim.replace(', ', '')
            rep_dict['TABLE_NAME'] = f'das_extra_dim_results_all{cc}_{extra_dim_clean}_{rep_dict['DAYS_BACK_START']}_{rep_dict['DAYS_BACK_END']}'

            if not do_calc_queries:
                query = open(os.path.join(sys.path[0], f"extra_dims_query{cc}.sql"), "r").read()
                print(f'doing: {cc}, {extra_dim}')
                get_bq_data(query, rep_dict)

            query = (f"select sum(revenue_no_domain) revenue_no_domain, sum(revenue_domain) revenue_domain "
                     f"from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`")
            mean_revenues = get_bq_data(query).iloc[0]

            NN = 1000

            hist_df_list = []
            for rand_prop in [0.1, 0.25, 0.5, 0.75]:
                query = "select "
                query += ",".join(np.repeat(f"sum(if(rand()< {rand_prop},coalesce(revenue_domain_opt_rps,revenue_domain),revenue_domain))", NN))
                query += f" from `sublime-elixir-273810.ds_experiments_us.{rep_dict['TABLE_NAME']}`"
                revenue_estimates = get_bq_data(query).iloc[0, :].values
                perc_uplift = (revenue_estimates / mean_revenues['revenue_no_domain'] - 1) * 100

                bin_counts, bins, _ = plt.hist(perc_uplift, bins=100, density=True, cumulative=True)
                hist_df_list.append(pd.DataFrame(bin_counts, index=pd.Index(bins[1:]), columns=[f'rand_perc_{rand_prop*100:0.0f}']))

            uplift_df = pd.concat(hist_df_list).sort_index().bfill().ffill()
            fig, ax = plt.subplots(figsize=(12, 9))
            uplift_df.plot(ax=ax, xlabel='Revenue uplift in %', ylabel='Cumulative probability of uplift shown on x-axis',
                           title=f'Revenue uplift estimate with uncertainty estimates for adding {extra_dim_clean}')
            fig.savefig(f'plots/revenue_variation{cc}_{extra_dim_clean}.png')

def main_expt_vs_opt():
    query = open(os.path.join(sys.path[0], f"expt_vs_opt.sql"), "r").read()
    zzz = get_bq_data(query)

    bin_counts, bins, _ = plt.hist(zzz.iloc[:,0].values, bins=100, density=True, cumulative=True)
    df = pd.DataFrame(bin_counts, index=pd.Index(bins[1:]))

    fig, ax = plt.subplots(figsize=(12, 9))
    df.plot(ax=ax, xlabel='% optimised rps is greater than experiment rps', ylabel='Cumulative proportion',
            xlim=[-10, 50], title=f"total cohort: {len(zzz)}")
    fig.savefig(f'plots/expt_vs_opt.png')

def main_expt_vs_opt_floors_hour():
    query = open(os.path.join(sys.path[0], f"expt_vs_opt_floors_hour.sql"), "r").read()
    zzz = get_bq_data(query)
    j = 0

    floors_hours = zzz['floors_hour'].unique()

    df_list = []
    for fh in floors_hours:
        zzz_fh = zzz[zzz['floors_hour'] == fh]
        bin_counts, bins, _ = plt.hist(zzz_fh.iloc[:, 1].values, bins=100, density=True, cumulative=True)
        df_list.append(pd.DataFrame(bin_counts, index=pd.Index(bins[1:]), columns=[fh]))

    df = pd.concat(df_list, axis=1).sort_index().ffill().bfill()
    fig, ax = plt.subplots(figsize=(12, 9))
    df.plot(ax=ax, xlabel='% optimised rps is greater than experiment rps', ylabel='Cumulative proportion',
            xlim=[-30, 60], title=f"total cohort: {len(zzz)}, broken down floors_hour")
    fig.savefig(f'plots/expt_vs_opt_floors_hour.png')


if __name__ == "__main__":
    main()

    #main_bootstrap_rev()

    #main_expt_vs_opt()

#    main_expt_vs_opt_floors_hour()