import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def main():

    x = pd.read_csv('Fill Rate Ad Units 23 June.csv')
    ad_units = x['Ad Unit'].unique()

    ad_units = [a for a in ad_units if 'tagged' not in a.lower()]

    replacement_dict = {'select_clause': "    union all \n".join([f"  select '%{a}' as str\n" for a in ad_units])}

    query = open(os.path.join(sys.path[0], f"query_ad_new_ad_units_1.sql"), "r").read()
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))

    print('Copy and paste the queries into pgAdmin. If there are any NULL, you need to work out why, or remove them from the list. When you have no Nulls proceed to the nect step.')

    p = 0

    query = open(os.path.join(sys.path[0], f"query_ad_new_ad_units_2.sql"), "r").read()
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))

    print('Copy the queries into pgadmin and run. Check what you see and if you want to insert the results into the db uncomment the first line.')


    p = 0


if __name__ == "__main__":
    main()