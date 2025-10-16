import time

import pandas as pd
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime as dt
import pickle
from utils import main_process_csv, get_bq_data

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

project_id = "streamamp-qa-239417"
dataset_name = 'DAS_increment'
client = bigquery.Client(project=project_id)


def get_domains_from_collection_ids(collection_ids, datelist):
    min_page_hits = 5000

    start_date = datelist[0]
    end_date = datelist[-1] + dt.timedelta(days=1)
    print(f'searching for domains for collection_ids: {", ".join(collection_ids)} from {start_date} to {end_date}, min_page_hits: {min_page_hits}')

    repl_dict = {'start_date': start_date.strftime("%Y-%m-%d"),
                 'end_date': end_date.strftime("%Y-%m-%d"),
                 'collection_ids_list': f"('{"', '".join(collection_ids)}')",
                 'min_page_hits': min_page_hits}

    query = open(os.path.join(sys.path[0], "queries/query_get_domains_from_collection_ids.sql"), "r").read()
    df = get_bq_data(query, client, repl_dict)
    return df['domain'].to_list()


def get_domains_from_test_names(test_names, datelist):
    min_page_hits = 10000

    start_date = datelist[0]
    end_date = datelist[-1] + dt.timedelta(days=1)
    print(f'searching for domains for test_names: {", ".join(test_names)} from {start_date} to {end_date}')

    repl_dict = {'start_date': start_date.strftime("%Y-%m-%d"),
                 'end_date': end_date.strftime("%Y-%m-%d"),
                 'test_names_list': f"('{"', '".join(test_names)}')",
                 'min_page_hits': min_page_hits}

    query = open(os.path.join(sys.path[0], "queries/query_get_domains_from_test_names.sql"), "r").read()
    df = get_bq_data(query, client, repl_dict)
    return df['domain'].to_list()


def does_table_exist(tablename):
    query = f"select count(*) from DAS_increment.INFORMATION_SCHEMA.TABLES where table_catalog || '.' || table_schema || '.' || table_name = '{tablename}'"
    df = get_bq_data(query, client)
    return bool(df.values[0, 0] > 0)



def main_process_data(query_filename, name, datelist, test_domains, minimum_sessions=2000, force_recreate_table=False):

    tablename = f"{project_id}.{dataset_name}.{query_filename.replace('query_', '')}_results"
    first_row = force_recreate_table or (not does_table_exist(tablename))

    domain_list = f"('{"', '".join(test_domains)}')"
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    print(f'query_filename: {query_filename} for domain_list: {domain_list}')

    for date in datelist.tolist():
        create_or_insert_statement = f"delete from `{tablename}` where date='{date.strftime("%Y-%m-%d")}' and ab_test_name='{name}'; insert into `{tablename}`"
        if first_row:
            create_or_insert_statement = f"CREATE OR REPLACE TABLE `{tablename}` as"
        first_row = False

        print(f'date: {date}: {create_or_insert_statement}')

        repl_dict = {'ddate': date.strftime("%Y-%m-%d"),
                     'create_or_insert_statement': create_or_insert_statement,
                     'domain_list': domain_list,
                     'name': name,
                     'minimum_sessions': minimum_sessions}
        get_bq_data(query, client, repl_dict)

def main_data_explore():
    date ='2025-08-20'
    query_filename = 'query_FI_AB_test_performance_data_explore'
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()
    get_bq_data(query, client, replacement_dict={'ddate': date})

    query = f'select * from `streamamp-qa-239417.DAS_increment.FI_AB_test_performance_raw_data_all_sites_{date}_explore order by 1, 2, 3, 4;'
    df = get_bq_data(query, client)
    df.transpose().to_csv('AB_data_6.csv')

def main_data_explore_date_range():
    query_filename = 'query_FI_AB_test_performance_data_explore'
    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()

    tablename_out = 'streamamp-qa-239417.DAS_increment.FI_AB_test_performance_raw_data_all_sites_explore'

    datelist = pd.date_range(start=dt.date(2025, 8, 16), end=dt.date(2025, 8, 20))
    first_row = True
    for date in datelist.tolist():
        date_str = date.strftime("%Y-%m-%d")
        print(f'date: {date}, query_filename: {query_filename}')
        get_bq_data(query, client, replacement_dict={'ddate': date_str})

        tablename_in = f'streamamp-qa-239417.DAS_increment.FI_AB_test_performance_raw_data_all_sites_{date_str}_explore'
        create_or_insert_query = (f"delete from `{tablename_out}` where date='{date_str}'; "
                                  f"insert into `{tablename_out}` ")
        if first_row:
            create_or_insert_query = f"CREATE OR REPLACE TABLE `{tablename_out}` as "
        create_or_insert_query += f"select '{date_str}' date, * from `{tablename_in}`"
        first_row = False
        print(f'date: {date}, {create_or_insert_query}')
        get_bq_data(create_or_insert_query, client)



def main(clean_db_and_backfill_data_from_the_beginning=False):
    # QUERIES
    for query_filename in ['query_FI_AB_test_performance', 'query_bidder_impact']:

        yesterday = dt.datetime.today().date() - dt.timedelta(days=1)
        three_days_ago = dt.datetime.today().date() - dt.timedelta(days=3)
        datelist = pd.date_range(start=three_days_ago, end=yesterday)

        if clean_db_and_backfill_data_from_the_beginning:
            datelist = None

        datelist = None
        #datelist = pd.date_range(start=dt.date(2025, 10, 13), end=yesterday)

        # Gamera Test
        name = 'gamera'
        if datelist is None:
            datelist = pd.date_range(start=dt.date(2025, 9, 20), end=yesterday)
        test_domains = get_domains_from_test_names(['78e690ca-fb19-4c37-8b6e-afd433446ac3', 'ef66fdaa-469a-407c-909b-d451e8815dbd', 'c924ff7e-f26c-489e-bdb1-74d4536b897e', '840828c2-05dd-4521-b584-ec2200016973'], datelist)
        main_process_data(query_filename, name, datelist, test_domains, force_recreate_table=clean_db_and_backfill_data_from_the_beginning)

        # # TRANSPARENT FLOORS larger test enforced
        # name = 'transparent_floors_sept_16_enforced'
        # if datelist is None:
        #     datelist = pd.date_range(start=dt.date(2025, 9, 16), end=yesterday)
        # test_domains = get_domains_from_collection_ids(['38622a20-b851-40d0-8c4a-ab2ab881fb0a'], datelist)  # enforced
        # main_process_data(query_filename, name, datelist, test_domains, force_recreate_table=clean_db_and_backfill_data_from_the_beginning)
        #
        # # TRANSPARENT FLOORS larger test not enforced
        # name = 'transparent_floors_sept_16_not_enforced'
        # if datelist is None:
        #     datelist = pd.date_range(start=dt.date(2025, 9, 16), end=yesterday)
        # test_domains = get_domains_from_collection_ids(['b2df7b52-27dc-409d-9876-0d945bad6f6e'], datelist)  # not enforced
        # main_process_data(query_filename, name, datelist, test_domains, force_recreate_table=clean_db_and_backfill_data_from_the_beginning)

        # #TRANSPARENT FLOORS original sites
        # name = 'transparent_floors_first_test_not_enforced'
        # if datelist is None:
        #     datelist = pd.date_range(start=dt.date(2025, 8, 16), end=dt.date(2025, 9, 26))
        # test_domains = [
        #     'baseball-reference.com',
        #     'deepai.org',
        #     'adsbexchange.com'
        # ]
        # main_process_data(query_filename, name, datelist, test_domains, force_recreate_table=clean_db_and_backfill_data_from_the_beginning)

        # name = 'transparent_floors_first_test_enforced'
        # if datelist is None:
        #     datelist = pd.date_range(start=dt.date(2025, 8, 16), end=dt.date(2025, 9, 26))
        # test_domains = [
        #     'pro-football-reference.com',
        #     'signupgenius.com',
        #     'worldofsolitaire.com',
        #     'deckshop.pro'
        # ]
        # main_process_data(query_filename, name, datelist, test_domains, force_recreate_table=clean_db_and_backfill_data_from_the_beginning)
        #
        # # # TIMEOUTS
        # name = 'timeouts_sept11'
        # if datelist is None:
        #     datelist = pd.date_range(start=dt.date(2025, 9, 11), end=dt.date(2025,9,18))
        # test_domains = get_domains_from_collection_ids(['5b60cd25-34e3-4f29-b217-aba2452e89a5'], datelist)
        # main_process_data(query_filename, name, datelist, test_domains, force_recreate_table=clean_db_and_backfill_data_from_the_beginning)
        #
        # name = 'timeouts_original'
        # if datelist is None:
        #     datelist = pd.date_range(start=dt.date(2025,8,26), end=dt.date(2025,9,15))
        # test_domains = get_domains_from_collection_ids(['9c42ef7c-2115-4da9-8a22-bd9c36cdb8b4', '5b60cd25-34e3-4f29-b217-aba2452e89a5'], datelist)
        # main_process_data(query_filename, name, datelist, test_domains, force_recreate_table=clean_db_and_backfill_data_from_the_beginning)

        tablename = f"{project_id}.{dataset_name}.{query_filename.replace('query_', '')}_results"
        main_process_csv(tablename, query_filename, client)


def main_timeouts(force_recreate_table=False):
    # query_filename = 'query_FI_timeouts_performance'
    # tablename = 'streamamp-qa-239417.DAS_increment.FI_timeouts_performance_results'

    query_filename = 'query_FI_timeouts_detailed'
    tablename = 'streamamp-qa-239417.DAS_increment.FI_timeouts_performance_results_detailed_not_refresh0'

    # query_filename = 'query_FI_timeouts_performance_2'
    # tablename = 'streamamp-qa-239417.DAS_increment.FI_timeouts_performance_results_2'

    query = open(os.path.join(sys.path[0], f"queries/{query_filename}.sql"), "r").read()

    #datelist = pd.date_range(start=dt.date(2025, 10, 1), end=dt.date(2025, 10, 8))
    datelist = pd.date_range(start=dt.date(2025, 9, 12), end=dt.date(2025, 9, 18))
#    datelist = pd.date_range(start=dt.date(2025, 8, 1), end=dt.date(2025, 10, 8))

    first_row = force_recreate_table or (not does_table_exist(tablename))
    for date in datelist.tolist():
        create_or_insert_statement = f"delete from `{tablename}` where date='{date.strftime("%Y-%m-%d")}'; insert into `{tablename}`"
        if first_row:
            create_or_insert_statement = f"CREATE OR REPLACE TABLE `{tablename}` as"
        first_row = False

        date_str = date.strftime("%Y-%m-%d")
        print(f'date: {date}, query_filename: {query_filename}, {create_or_insert_statement}')

        get_bq_data(query, client, replacement_dict={'ddate': date_str, 'create_or_insert_statement': create_or_insert_statement})

if __name__ == "__main__":

    main()

    #main_data_explore_date_range()

#    main_timeouts()