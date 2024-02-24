import pandas as pd
import glob
import os


# Get most recent snapshot from BQ
def get_latest_file(dir, filetype):
    # Returns the full file path 
    list_of_files = glob.glob(f'{dir}/*{filetype}')
    latest_file = max(list_of_files, key=os.path.getctime)
    print(latest_file)
    return latest_file

def get_earliest_file(dir, filetype):
    # Returns the full file path 
    list_of_files = glob.glob(f'{dir}/*{filetype}')
    earliest_file = min(list_of_files, key=os.path.getctime)
    print(earliest_file)
    return earliest_file

def snapshot_exists():
    if 'snapshot_' in os.listdir('data/4_snapshots'):
        return True
    else:
        return False

def create_updated_snapshot(prev_snapshot_listings: pd.DataFrame, daily_listings: pd.DataFrame) -> pd.DataFrame:

        daily_listings['date_found'] = pd.to_datetime(daily_listings['date_found']).dt.date
        daily_listings['approx_date_listed'] = pd.to_datetime(daily_listings['approx_date_listed']).dt.date
        daily_listings['latest_date_found'] = daily_listings['date_found']

        print(daily_listings.dtypes)
        
        new_listings = daily_listings[~daily_listings['id'].isin(prev_snapshot_listings['id'])]

        existing_listings = daily_listings[daily_listings['id'].isin(prev_snapshot_listings['id'])]


        # Merge DataFrames on 'id' using a left join
        snapshot = prev_snapshot_listings.merge(existing_listings[['id','latest_date_found']], on='id', how='left', suffixes=('_prev_snapshot', '_latest_listings'))

        # Coalesce the 'last_date' columns
        snapshot['latest_date_found'] = snapshot['latest_date_found_latest_listings'].combine_first(snapshot['latest_date_found_prev_snapshot'])
        snapshot = snapshot.drop(columns=['latest_date_found_latest_listings','latest_date_found_prev_snapshot'])
        
        # Union the new_listings to snapshot
        snapshot = pd.concat([snapshot,new_listings], axis = 0)
        snapshot['latest_date_found'] = pd.to_datetime(snapshot['latest_date_found']).dt.date
        snapshot['date_found'] = pd.to_datetime(snapshot['date_found']).dt.date
        snapshot['listing_alive_days'] = (snapshot['latest_date_found'] - snapshot['date_found'])/86400000000
        
        return snapshot

# Get the snapshot tabnle

if not snapshot_exists(): # Create the file from the oldest cleaned-zip
    
    prev_snapshot_file = get_earliest_file('data/3_cleaned-zip','.zip')
    prev_snapshot_listings = pd.read_csv(prev_snapshot_file)
    prev_snapshot_listings['approx_date_listed'] = pd.to_datetime(prev_snapshot_listings['approx_date_listed']).dt.date
    prev_snapshot_listings['date_found'] = prev_snapshot_listings['approx_date_listed']
    prev_snapshot_listings['latest_date_found'] = prev_snapshot_listings['approx_date_listed']
    # prev_snapshot_listings['latest_date_found'] = pd.to_datetime(prev_snapshot_listings['latest_date_found'])
    # prev_snapshot_listings['date_found'] = pd.to_datetime(prev_snapshot_listings['date_found'])
    prev_snapshot_listings['listing_alive_days'] = (prev_snapshot_listings['latest_date_found'] - prev_snapshot_listings['date_found'])/86400000000
    
    file_date = prev_snapshot_file.split('T')[0].split('cleaned_')[-1]
    prev_snapshot_listings.to_parquet(f'data/4_snapshots/snapshot_{file_date}.parquet')
    # for now, we have to make an asumption about the first 40k listings
    # later, we can truncate the rows where date_found < 2024-02-18...

daily_cleanedzip_ls = sorted(glob.glob('data/3_cleaned-zip/*.zip'))
# This loop will handle having multiple days of snapshot to calculate

#Kickstart
prev_snapshot_file = get_latest_file('data/4_snapshots','.parquet')
snapshot_file_date = prev_snapshot_file.split('.parquet')[0].split('snapshot_')[-1]

start_from_index = None
for i, f in enumerate(daily_cleanedzip_ls):
    if snapshot_file_date in f:
        start_from_index = i+1

for cleanedzip in daily_cleanedzip_ls[start_from_index:]:
    prev_snapshot_file = get_latest_file('data/4_snapshots','.parquet')
    snapshot_file_date = prev_snapshot_file.split('.parquet')[0].split('snapshot_')[-1]
    
    
    prev_snapshot_listings = pd.read_parquet(prev_snapshot_file) # Get the snapshot listing from the latest snapshot in the folder

    daily_listings = pd.read_csv(cleanedzip)
    daily_file_date = cleanedzip.split('T')[0].split('cleaned_')[-1]
    snapshot = create_updated_snapshot(prev_snapshot_listings, daily_listings)
    snapshot.to_parquet(f'data/4_snapshots/snapshot_{daily_file_date}.parquet')
