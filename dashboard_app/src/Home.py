# https://docs.streamlit.io/get-started/tutorials/create-a-multipage-app

import json
import os

import backend_bq
import pandas as pd
import streamlit as st

# INIT CONFIGS

data_dir = "./data/init"
MIN_N = 7
MAX_N = 28
expected_files = MAX_N // MIN_N
## FNS


def check_data_updated(
    data_dir: str = data_dir,
    expected_files: int = expected_files,
) -> bool:

    r = backend_bq.bq_execute_query(
        backend_bq.SQL_MAX_DT,
        dataset_id="pg_listings",
        table_id="agg_viz_group",
    )
    current_max_dt = r.iloc[0]["max_dt"]
    print(current_max_dt)
    fc = 0
    for filename in os.listdir(data_dir):
        if filename.endswith(".parquet"):
            if filename.startswith(current_max_dt):
                fc += 1

    if fc == expected_files:
        return True
    return False


def initialise_data(N_DAYS: int, data_dir=data_dir) -> pd.DataFrame:
    sql_format = backend_bq.SQL_BASIC_METRICS.format(N_DAYS=N_DAYS)
    df_init = backend_bq.bq_execute_query(
        sql_format,
        dataset_id="pg_listings",
        table_id="agg_viz_group",
    )
    # if is_all_region:
    #     df_r = df_r[df_r["region"] == "ALL"]
    # else:
    #     df_r = df_r[df_r["region"] != "ALL"]
    max_dt = df_init["dt"].max().strftime("%Y-%m-%d")
    print(max_dt)

    file_name = f"{data_dir}/{max_dt}_{N_DAYS}.parquet"
    df_init.to_parquet(file_name)
    return file_name, max_dt


def update_metadata(data_dir, files, max_dt):
    data = {"files": files, "dt": max_dt}
    fp = f"{data_dir}/metadata.json"
    with open(fp, "w") as json_file:
        json.dump(data, json_file, indent=4)
    print("Metadata updated")
    return fp


# STREAMLIT STUFF
st.set_page_config(
    page_title="Welcome!",
    page_icon="👋",
)

st.write("# Welcome to PG Listings Watcher!")


st.markdown(
    """
    This app shows the metrics of listings on PropertyGuru website. Data on all 50,000+ listings is collected daily and displayed here.
    
    Today's raw listing data is can be downloaded [here](https://www.oliverq.site/external/public_datasets/pg-scrape-auto/latest_daily.csv).
    
    Do drop me a message @ [oliverqsw@gmail.com](mailto:oliverqsw@gmail.com) if[oliverqsw@gmail.com you would like the entire set of daily listings starting from 03 Sep 2024.
"""
)


with st.status("Checking for latest data...", expanded=True) as status:
    if check_data_updated():
        print("Files are already the latest updated dt")
    else:
        print("Updating data files")
        st.write("Downloading data, please hang on!")
        files = []
        for i, N_DAYS in enumerate(range(MIN_N, MAX_N + 1, 7)):

            print(N_DAYS)
            file_name, max_dt = initialise_data(N_DAYS)
            files.append(file_name)
            status.update(label=f"Downloaded {i+1} of {expected_files} files")

        update_metadata(data_dir, files, max_dt)

    status.update(label="App is ready!", state="complete", expanded=False)

st.sidebar.success("Select a dashboard analysis above.")

print("App intialised!")
