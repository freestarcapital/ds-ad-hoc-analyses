import json
import pandas as pd
from google.cloud import storage



def gcs_json_to_dataframe(bucket_name, file_name):
    """
    Reads a JSON file from a GCS bucket and converts it into a pandas DataFrame.

    Args:
        bucket_name (str): The name of the GCS bucket.
        file_name (str): The path to the JSON file within the bucket.

    Returns:
        pandas.DataFrame or None: A DataFrame containing the data from the JSON file,
                                  or None if an error occurs.
    """
    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download the JSON file as a string
        json_string = blob.download_as_string()

        # Load the JSON data
        data = json.loads(json_string)

        # Use json_normalize to handle nested JSON and create a DataFrame
        df = pd.json_normalize(data)

        print(f"✅ Successfully created DataFrame from '{file_name}'.")
        return df

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return None


if __name__ == '__main__':
    BUCKET = "ideal-ad-stack"
    FILE = "fad-config.json"

    df_data = gcs_json_to_dataframe(BUCKET, FILE)

    if df_data is not None:
        print("\nDataFrame Head:")
        print(df_data.head())

