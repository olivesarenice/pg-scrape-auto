import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import os
import argparse



def processDetails(lst):
    property_type, lease_type, completed = '', '', ''
    for item in lst:
        if ':' in item:
            completed = item
        elif 'hold' in item or 'Tenure' in item:
            lease_type = item
        else:
            property_type = item
    return pd.Series([lease_type, completed ]) # drop property_type as its the same as the other column

def getTenure(x):
    
    if 'Leasehold' in x:
        return x.split("-")[0]
    elif 'Freehold' in x:
        return 999_999
    else:
        return 0



def convertUptimeDays(x):
# uptime intervals are: ['s', 'm', 'h', 'd', 'w', 'mon', 'yr']
    conversion = {'m':1/1440,
                  'h':1/24,
                  'd':1,
                  'w':7,
                  'mon':30,
                  'yrs':365,
                  'yr':365,
                  's':1/86400}

    for k,v in conversion.items():
        if x.endswith(k):
            return float(x[:x.find(k)]) * v
        
def getRemainingLease(completed, lease_tenure, timestamp):
    
    if lease_tenure > 0 and completed > 0:
        
        return lease_tenure - (timestamp.year - completed)
    else:
        return -1

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required = False, help="File name in data/2_processed-df/")
    args = parser.parse_args()
    print(args)
    filename = args.filename
    processed_folder = r"data/2_processed-df/"
    
    #start_time = datetime.utcnow()
    if filename == None:
    
        list_of_files = glob.glob(f'{processed_folder}*.csv') # * means all if need specific format then *.csv
        filename = os.path.basename(max(list_of_files, key=os.path.getctime))
        print(f'Using latest file: {filename}')
    
    start_time_file_name = filename.split("_")[0]    
    start_time = datetime.strptime(start_time_file_name,"%Y%m%dT%H%M%S")
    r_df = pd.read_csv(f"{processed_folder}{filename}")
        
    r_df['timestamp'] = start_time


    # is_turbo

    r_df['is_turbo'] = r_df['is_turbo'].apply(lambda x: 1 if 'Turbo' in x else 0).astype('Int64')
    r_df

    r_df['beds'] = r_df['beds'].astype('Int64')
    r_df['baths'] = r_df['baths'].astype('Int64')
    r_df['area_sqft']=r_df['area_sqft'].apply(lambda x: x.split(' ')[0] if isinstance(x, str) else np.nan)
    r_df['pg_project_id'] = r_df['pg_project_id'].astype('Int64')
    r_df['is_new_project']=r_df['is_new_project'].apply(lambda x: 1 if isinstance(x, str) else 0).astype('Int64')
    r_df['proximity_mins']=r_df['proximity_mins'].apply(lambda x: x.split(' mins')[0] if isinstance(x, str) else np.nan).astype('Int64')
    r_df['proximity_m']=r_df['proximity_m'].apply(lambda x: (x[x.find("(")+1:x.find(")")]).split(' m')[0] if isinstance(x, str) else np.nan).astype('Int64')
    r_df['proximity_to_mrt']=r_df['proximity_to_mrt'].apply(lambda x: x.split(' to ')[1] if isinstance(x, str) else np.nan)
    
    details_df = pd.DataFrame(r_df['property_details'].apply(eval).apply(processDetails).values.tolist(), columns = ['lease_type','completed'])
    details_df['completed'] = details_df['completed'].apply(lambda x: x.split(': ')[1] if len(x) > 0 else 0).astype(int)
    details_df['lease_tenure'] = details_df['lease_type'].apply(getTenure).astype(int)

    r_df = pd.concat([r_df, details_df], axis =1)
        
    r_df['remaining_lease'] = r_df.apply(lambda x: getRemainingLease(x.completed, x.lease_tenure, x.timestamp), axis = 1)
        
    r_df['listing_uptime_days'] = r_df['listing_uptime'].apply(convertUptimeDays)
    r_df['agent_id'] = r_df['agent_id'].astype('Int64')
    r_df['headline'] = r_df['headline'].str.replace('"','')
    
    r_df['date_found'] = start_time.date()
    r_df['approx_date_listed'] = r_df['listing_uptime_days'].apply(lambda x : (start_time - timedelta(seconds=int(round(x*86400)))).date())
    save_path = f"data/3_cleaned-zip/cleaned_{filename}.zip"
    r_df.to_csv(save_path,compression={'method': 'zip', 'archive_name': filename},index=False)
    print(f'...done! Data saved to {save_path}')

if __name__ == "__main__":
    main()