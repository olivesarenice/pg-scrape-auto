import pandas as pd
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from generate_snapshot import get_latest_file
import datetime
# Set your Google Cloud project and BigQuery dataset information
project_id = 'propguru'
dataset_id = 'test_snapshot'
table_id = 'snapshot_latest'

# Set the path to your service account key JSON file
service_account_key_path = get_latest_file('.env','.json')
print(service_account_key_path)
# Set the path to your Parquet file
parquet_file_path = get_latest_file('data/4_snapshots','.parquet')
print(f'FILE: {parquet_file_path}')
# Load the Parquet file into a Pandas DataFrame

df = pd.read_parquet(parquet_file_path, engine='pyarrow')
df['BQ_UPLOAD_TS'] = datetime.datetime.utcnow()
print(df.dtypes)
date_dicts = []
for column in df.columns:
    dtype = str(type(df[column].values[0]))
    if dtype == "<class 'datetime.date'>":
        df[column]  = pd.to_datetime(df[column])
        dtype_dict = {'name':column,'type':'DATE'}
        date_dicts.append(dtype_dict)
    if dtype == "<class 'numpy.timedelta64'>":
        df[column] = df[column].astype('int64').astype('int')
print(df.dtypes)
# Set up BigQuery credentials
credentials = service_account.Credentials.from_service_account_file(
    service_account_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
print(f'Loading {len(df)} rows ...')
# Write the DataFrame to a BigQuery table
to_gbq(df, f'{project_id}.{dataset_id}.{table_id}', if_exists='replace', credentials=credentials, table_schema = date_dicts)
print(f'Data loaded to {project_id}.{dataset_id}.{table_id}')