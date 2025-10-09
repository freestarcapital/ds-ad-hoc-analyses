import json
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud.exceptions import NotFound
from typing import Dict, Any, List
from datetime import date

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Initialize clients once at the top level
bq_client = bigquery.Client()
bqstorageclient = bigquery_storage.BigQueryReadClient()

# Define the dictionary of project IDs once
REPL_DICT = {
    "project_id_network_config": "freestar-157323",
    "project_id_site_network_config": "freestar-prod",
    "project_id_onboarding_info": "sublime-elixir-273810",
    "dest_domain_bidder_status": {
        "project_id": "streamamp-qa-239417",
        "dataset_name": "FAD_configs",
        "table_name": "domain_bidder_status"
    },
    "dest_network_bidders": {
        "project_id": "streamamp-qa-239417",
        "dataset_name": "FAD_configs",
        "table_name": "network_bidders"
    },
    "dest_allowed_geos": {
        "project_id": "streamamp-qa-239417",
        "dataset_name": "FAD_configs",
        "table_name": "allowed_geos"
    },
    "dest_onboarding_raw": {
        "project_id": "streamamp-qa-239417",
        "dataset_name": "FAD_configs",
        "table_name": "onboarding_raw"
    },
    "dest_onboarding_expanded": {
        "project_id": "streamamp-qa-239417",
        "dataset_name": "FAD_configs",
        "table_name": "onboarding_expanded"
    }
}
def get_bq_data(query: str, repl_dict: Dict[str, Any]) -> pd.DataFrame:
    for k, v in repl_dict.items():
        query = query.replace("{" + k + "}", str(v))

    try:
        df = bq_client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient)
        print("✅ Query executed successfully and data loaded into DataFrame.")
        return df
    except Exception as e:
        print(f"❌ An error occurred during query execution: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure

# --- Corrected and simplified data retrieval functions ---

def get_dashboard_network_data() -> pd.DataFrame:
    """Retrieves data from the ad_networks table."""
    query = """
    SELECT slug FROM {project_id_network_config}.dashboard.ad_networks
    WHERE active AND prebid_server_enabled = false
    """
    print(' - - - Querying dashboard.ad_networks - - -')
    df = get_bq_data(query, REPL_DICT)
    return df


def get_dashboard_site_network_data() -> pd.DataFrame:
    """Retrieves data from the network_config table."""
    query = """
    WITH latest_data AS (
      SELECT 
        site_id,
        MAX(processing_date) AS latest_processing_date
      FROM 
        {project_id_site_network_config}.pubfig_site_configurations.network_config
      GROUP BY 
        site_id
    )
    SELECT 
      s.domain, nc.slug, nc.is_client, nc.is_prebid_server, nc.is_client_and_server, nc.processing_date
    FROM 
      {project_id_site_network_config}.pubfig_site_configurations.network_config nc
    JOIN 
      latest_data ld
    ON 
      nc.site_id = ld.site_id AND nc.processing_date = ld.latest_processing_date
    JOIN {project_id_network_config}.dashboard.sites s on s.id = nc.site_id
    WHERE s.inactive IS FALSE
    """
    print(' - - - Querying pubfig_site_configurations.network_config - - -')
    df = get_bq_data(query, REPL_DICT)
    return df


def get_allowed_geo_data() -> pd.DataFrame:
    """Retrieves data from the network_allowed_geos table."""
    query = """
    SELECT an.slug, geo
    FROM {project_id_network_config}.dashboard.network_allowed_geos ag
    JOIN {project_id_network_config}.dashboard.ad_networks an
    ON ag.network_id = an.id
    """
    print(' - - - Querying dashboard.network_allowed_geos - - -')
    df = get_bq_data(query, REPL_DICT)
    return df


def get_onboarding_info() -> pd.DataFrame:
    """Retrieves data from the domain_bidder_onboarding table."""
    query = """
    SELECT * FROM {project_id_onboarding_info}.ideal_ad_stack.domain_bidder_onboarding
    """
    print(' - - - Querying ideal_ad_stack.domain_bidder_onboarding - - -')
    df = get_bq_data(query, REPL_DICT)
    return df


def generate_multiple_dataframes(network_data,site_network_data, geo_data, onboarding_info_data):
    print("✨ Generating multiple DataFrames...")

    # DataFrame 1: Bidder status at the network level
    df_network_data = network_data.copy()

    # DataFrame 2: Bidder status at the domain level
    df_domain_bidder_status = site_network_data.copy()
    df_domain_bidder_status.rename(columns={'slug': 'bidder'}, inplace=True)

    # Fill NaN values and convert to boolean to prevent conversion errors
    df_domain_bidder_status['is_client'] = df_domain_bidder_status['is_client'].fillna(False).astype(bool)
    df_domain_bidder_status['is_prebid_server'] = df_domain_bidder_status['is_prebid_server'].fillna(False).astype(bool)
    df_domain_bidder_status['is_client_and_server'] = df_domain_bidder_status['is_client_and_server'].fillna(
        False).astype(bool)

    conditions = [
        df_domain_bidder_status['is_client'],
        df_domain_bidder_status['is_prebid_server'],
        df_domain_bidder_status['is_client_and_server']
    ]
    choices = [
        'clientOnlyBiddersDomainLevel',
        'serverOnlyBiddersDomainLevel',
        'clientAndServerBiddersDomainLevel'
    ]
    df_domain_bidder_status['bidder_type'] = np.select(conditions, choices, default='unknown')

    # df_domain_bidder_status = df_domain_bidder_status.drop(
    #     columns=['is_client', 'is_prebid_server', 'is_client_and_server'])
    df_domain_bidder_status['processing_date'] = pd.to_datetime(df_domain_bidder_status['processing_date'])

    # DataFrame 3: Allowed Geos
    df_allowed_geos = geo_data.copy()
    df_allowed_geos.rename(columns={'slug':'bidder'},inplace=True)
    # DataFrame 4: Onboarding Info
    # DataFrame 3: Onboarding Info - Now restructured to have bidders as columns
    df_onboarding_raw = onboarding_info_data.copy()

    # Explode the configuredBidders list to create one row per bidder
    df_onboarding_expanded = df_onboarding_raw.explode('configuredBidders').reset_index(drop=True)

    # Extract bidder slug and 'new' status from the dictionary
    if not df_onboarding_expanded.empty:
        df_onboarding_expanded['bidder'] = df_onboarding_expanded['configuredBidders'].apply(lambda x: x['slug'])
        df_onboarding_expanded['is_new'] = df_onboarding_expanded['configuredBidders'].apply(lambda x: x['new'])
    else:
        df_onboarding_expanded['bidder'] = None
        df_onboarding_expanded['is_new'] = None

    df_onboarding_raw.rename(columns={'configuredBidders':'configured_bidders',},inplace=True)
    df_onboarding_expanded.rename(columns={'configuredBidders': 'configured_bidders',
                                           'bidder_slug': 'bidder' }, inplace=True)

    # Add a 'run_date' to all DataFrames to mark this specific data snapshot
    run_date = date.today()
    df_network_data['run_date']=run_date
    df_domain_bidder_status['run_date'] = run_date
    df_allowed_geos['run_date'] = run_date
    df_onboarding_raw['run_date'] = run_date
    df_onboarding_expanded['run_date'] = run_date

    print("✅ All DataFrames generated successfully.")

    return {
        'network_data': df_network_data,
        'domain_bidder_status': df_domain_bidder_status,
        'allowed_geos': df_allowed_geos,
        'onboarding_raw': df_onboarding_raw ,
        'onboarding_expanded': df_onboarding_expanded
    }

def save_dataframe_to_bigquery(df, project_id, table_name, dataset_name="FAD_configs"):

    # project_id='streamamp-qa-239417'
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    if df.empty:
        print(f"⚠️ DataFrame for {table_id} is empty, skipping save to BigQuery.")
        return

    print(f"🚀 Saving DataFrame to BigQuery table: {table_id}...")
    try:
        # Check if the table exists to decide on the write disposition
        bq_client.get_table(table_id)
        # If the table exists, append the data
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        print(f"Table '{table_id}' found, appending data...")
    except NotFound:
        # If the table does not exist, create it by truncating (effectively creating a new table)
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        print(f"Table '{table_id}' not found, creating new table...")

    try:
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"✅ Data saved successfully to {table_id}")
    except Exception as e:
        print(f"❌ An error occurred while saving to BigQuery: {e}")

# --- Example Usage ---
if __name__ == '__main__':
    network_df = get_dashboard_network_data()
    site_network_df = get_dashboard_site_network_data()
    geo_df = get_allowed_geo_data()
    onboarding_info_df = get_onboarding_info()

    if not all([not df.empty for df in [network_df, site_network_df, geo_df, onboarding_info_df]]):
        print("\nOne or more DataFrames failed to load. Exiting.")
    else:
        # Step 2: Generate the granular DataFrames
        generated_dfs = generate_multiple_dataframes(network_df, site_network_df, geo_df, onboarding_info_df)

        # Print a sample of each generated DataFrame for verification
        for name, df in generated_dfs.items():
            print(f"\nDataFrame '{name}' Head:")
            print(df.head())

        # Step 3: Save each DataFrame to a designated BigQuery table
        network_table_info = REPL_DICT.get("dest_network_bidders")
        save_dataframe_to_bigquery(network_df, **network_table_info)

        domain_bidder_status_info = REPL_DICT.get("dest_domain_bidder_status")
        save_dataframe_to_bigquery(generated_dfs['domain_bidder_status'], **domain_bidder_status_info)

        allowed_geos_info = REPL_DICT.get("dest_allowed_geos")
        save_dataframe_to_bigquery(generated_dfs['allowed_geos'], **allowed_geos_info)

        onboarding_raw = REPL_DICT.get("dest_onboarding_raw")
        save_dataframe_to_bigquery(generated_dfs['onboarding_raw'], **onboarding_raw)

        onboarding_expanded = REPL_DICT.get("dest_onboarding_expanded")
        save_dataframe_to_bigquery(generated_dfs['onboarding_expanded'], **onboarding_expanded)

