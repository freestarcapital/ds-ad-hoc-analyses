
import dateutil.utils
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime
import pickle


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

np.set_printoptions(suppress=True, linewidth=10000)


project_id = "freestar-157323"
client = bigquery.Client(project=project_id)#, location='EU')
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main():
    query = open(os.path.join(sys.path[0], f"query_floor_uplift_base.sql"), "r").read()
    get_bq_data(query)



if __name__ == "__main__":
    main()