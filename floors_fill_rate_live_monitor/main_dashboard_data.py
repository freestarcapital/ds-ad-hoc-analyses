import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
from matplotlib.backends.backend_pdf import PdfPages
import datetime as dt
from sklearn.linear_model import LinearRegression

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

project_id = "sublime-elixir-273810"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()


def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')


def main_dashboard_only(results_tablename, recreate_raw_data=False, print_reference_units=False):
    if recreate_raw_data:
        query = open(os.path.join(sys.path[0], f"query_create_raw_data.sql"), "r").read()
        get_bq_data(query,{'table_name': 'sublime-elixir-273810.training_fill_rate.base_data_for_performance_checking',
                           'WINDOW_DAYS': 90})
    # return

    query_dashboard = open(os.path.join(sys.path[0], f"query_get_perf_from_base_data_for_dashboard.sql"), "r").read()
    query_reference_ad_units = open(os.path.join(sys.path[0], f"query_get_reference_ad_units.sql"), "r").read()

    ad_units = pd.read_csv('fill-rate-ads.csv')
    print('read fill-rate-ads.csv')

    first_row = True
    for i, (_, (ad_unit, domain, working, fill_rate_model_enabled_date_str)) in enumerate(ad_units.iterrows()):

        if (',' in ad_unit) or ('test' in ad_unit) or not working:
            print(f'skipping: {ad_unit}')
            continue

        fill_rate_model_enabled_date = dt.datetime.strptime(fill_rate_model_enabled_date_str,'%d/%m/%Y').strftime('%Y-%m-%d')

        create_or_insert_statement = f"CREATE OR REPLACE TABLE `{results_tablename}` as" if first_row else f"insert into `{results_tablename}`"
        first_row = False

        print(f"ad_unit: {ad_unit}, fill_rate_model_enabled_date: {fill_rate_model_enabled_date}, {i} of {len(ad_units)}")

        reference_ad_units_where = f"ad_unit_name like '{ad_unit.split('_')[0]}\\\\_%'"
        for ad_unit_other in ad_units[ad_units['domain'] == domain]['ad_unit']:
            reference_ad_units_where += f" and ad_unit_name != '{ad_unit_other}'"

        repl_dict = {'ad_unit': ad_unit,
                     'reference_ad_units_where': reference_ad_units_where,
                     'create_or_insert_statement': create_or_insert_statement,
                     'start_date': "2025-05-1",
                     'fill_rate_model_enabled_date': fill_rate_model_enabled_date}

        if print_reference_units:
            df_reference_ad_units = get_bq_data(query_reference_ad_units, repl_dict)
            print(df_reference_ad_units)

        get_bq_data(query_dashboard, repl_dict)


def do_scatterplot(x, y, c, ax_):
    ax_.scatter(x, y, c=c)
    coef = LinearRegression(fit_intercept=False).fit(x.to_frame(), y).coef_[0]
    x_max = x.max()
    ax_.plot([0, x_max], [0, x_max*coef], c)
    return coef

def main_summary_plots_from_query(results_tablename):

    min_daily_ad_requests = 3000
    before_and_after_analysis_days = 28

    #dims = "ad_unit, country_code, device_category"
    dims = "ad_unit"

    query = open(os.path.join(sys.path[0], f"query_main_summary_plots.sql"), "r").read()
    df = get_bq_data(query, {'dims': dims, 'min_daily_ad_requests': min_daily_ad_requests,
        'before_and_after_analysis_days': before_and_after_analysis_days, 'results_tablename': results_tablename})
    df = df[[c for c in df.columns if c not in dims]].astype('float64')
    df = df[~df.isna().any(axis=1)]

    plot_bases = [('ad_request_weighted_floor_price', 1, 1), ('fill_rate', 0.7, 0.8), ('cpm', 2.5, 2.5), ('cpma', 1.3, 1.3)]
    fig, ax = plt.subplots(figsize=(12, 9), ncols=2, nrows=2)
    fig.suptitle(f'Fill-rate model analysis: blue: fill-rate model, red: cpma-max model, black: unity gradient line, {len(df)} points')
    ax = ax.flatten()
    for i, (pb, x_max, y_max) in enumerate(plot_bases):
        ax_ = ax[i]
        coeff_fr = do_scatterplot(df[f'{pb}_fr_before'], df[f'{pb}_fr_after'], 'b', ax_)
        coeff_rm = do_scatterplot(df[f'{pb}_rm_before'], df[f'{pb}_rm_after'], 'r', ax_)

        xy_max = min(x_max, y_max)
        ax_.plot([0, xy_max], [0, xy_max], 'k--')
        ax_.set_title(f'{pb}, coeff_rm: {coeff_rm:0.2f}, coeff_fr: {coeff_fr:0.2f}')
        ax_.set_xlabel(f'average {before_and_after_analysis_days} days before')
        ax_.set_ylabel(f'average {before_and_after_analysis_days} days after')
        ax_.set_xlim([0, x_max])
        ax_.set_ylim([0, y_max])

    plotname = f'plots/plot_fill_rate_performance_{dims.replace(',', '_').replace(' ', '')}_{before_and_after_analysis_days}_{min_daily_ad_requests}'
    fig.savefig(f'{plotname}_1.png')

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(df['ad_request_weighted_floor_price_fr_after'], df['fill_rate_fr_after'])
    ax.set_xlabel('floor price')
    ax.set_ylabel('fill rate')
    fig.savefig(f'{plotname}_2.png')

def main_create_summary_results_table(results_tablename):

    before_analysis_days = 42
    start_after_analysis_days = 1
    end_after_analysis_days = before_analysis_days

    query = open(os.path.join(sys.path[0], f"query_main_summary_table.sql"), "r").read()
    get_bq_data(query, {'dims': "ad_unit, country_code, device_category",
                        'before_analysis_days': before_analysis_days,
                        'start_after_analysis_days': start_after_analysis_days,
                        'end_after_analysis_days': end_after_analysis_days,
                        'results_tablename': results_tablename,
                        'summary_tablename': f'{results_tablename}_summary'})
#                        'summary_tablename': f'{results_tablename}_summary_before_{before_analysis_days}_after_{start_after_analysis_days}_{end_after_analysis_days}'})

if __name__ == "__main__":
    results_tablename = 'sublime-elixir-273810.training_fill_rate.fill-rate_results_for_performance_checking'
    main_dashboard_only(results_tablename, recreate_raw_data=False)
    #main_summary_plots_from_query(results_tablename)
    #main_create_summary_results_table(results_tablename)