import datetime

import altair as alt
import backend_bq
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib 

N_DAYS = 20
def load_data(is_all_region: bool) -> pd.DataFrame:
    sql_format = backend_bq.SQL_BASIC_METRICS.format(N_DAYS=N_DAYS)
    df_r = backend_bq.bq_execute_query(
        sql_format,
        dataset_id="pg_listings",
        table_id="agg_viz_group",
    )
    if is_all_region:
        df_r = df_r[df_r["region"] == "ALL"]
    else:
        df_r = df_r[df_r["region"] != "ALL"]
    return df_r


def parse_df_row(row: pd.DataFrame) -> dict:
    if len(row) != 1:
        return TypeError
    row = row.iloc[0]
    row_d = {}
    row_d["description"] = row["description"].split(" | ")[1]
    row_d["psf"] = f"${str(int(round(row["median_psf"],0)))}"
    row_d["psf_delta"] = f"{str(round(row["psf_pct_delta"] * 100, 1))}%"
    row_d["psf_h"] = row["median_psf_history"].tolist()
    row_d["listing"] = row["listings"]
    row_d["listing_delta"] = f"{str(round(row["listings_pct_delta"] * 100, 1))}%"
    row_d["listing_h"] = row["listings_history"].tolist()
    return row_d


df_r = load_data(is_all_region=False)
partition_date = df_r["dt"].max()

print(partition_date)

df_r.to_csv("sample_data.csv")
print(df_r)
st.set_page_config(page_title="HDB Listings by Region", page_icon="🏢")

# Set configs

# https://discuss.streamlit.io/t/how-to-remove-expansion-arrow-in-altair-chart/40862/2
st.markdown(
    """
    <style>
    button[title="View fullscreen"] {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Intro
st.markdown("# HDB by Region")
st.sidebar.header("HDB by Region")
st.write(
    """This page shows the median PSF, number of listings, and median lifetime of listings for each HDB type, split by regions"""
)

df_r["median_psf"] = df_r["median_psf"].apply(round)
df_r["psf_pct_delta"] = df_r["psf_pct_delta"].apply(lambda x: f"{x:+.1%}")
df_r["listings_pct_delta"] = df_r["listings_pct_delta"].apply(lambda x: f"{x:+.1%}")
df_r["p_history"] = df_r["median_psf_history"].apply(lambda x: [i["value"] for i in list(x)])
df_r["l_history"] = df_r["listings_history"].apply(lambda x: [i["value"] for i in list(x)])

# for HDB

segment_code_map = {
    "HDB": ["AH2","AH3","AH4","AH5","AH0"],
    "NL": ["AN1","AN2","AN3","AN4","AN5"],
    "L": ["AL1","AL2","AL3"],
}

for segment, types in segment_code_map.items():
    st.markdown("## Median PSF")
    df_segment = df_r[df_r["viz_group_code"].isin(types)]
    df_piv = df_segment.pivot_table(index=["region"], columns='description',values="median_psf")
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap='bone', subset=df_piv.columns,vmin=min_value, vmax=max_value*2).format(precision=0, thousands=",")
    st.dataframe(df_styled)

    st.markdown("## Number of listings")
    df_piv = df_segment.pivot_table(index=["region"], columns='description',values="listings")
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap='bone', subset=df_piv.columns,vmin=min_value, vmax=max_value*2).format(precision=0, thousands=",")
    st.dataframe(df_styled)

    st.markdown("## Trends by Type")
    for t in types:
        st.markdown(f"### {t}")
        df = df_r[df_r["viz_group_code"] == t]
        display_cols = ["region",
                        "median_psf",
                        "psf_pct_delta",
                        "p_history",
                        "listings",
                        "listings_pct_delta",
                        "l_history"]
        df = df[display_cols]
        st.dataframe(
            df,
            column_config={
                "region": "Region",
                "median_psf": "PSF",
                "psf_pct_delta": f"{N_DAYS}D Δ",
                "p_history": st.column_config.LineChartColumn(
                    f"past {N_DAYS}D", width="small"
                ),
                "listings": "Listings",
                "listings_pct_delta": f"{N_DAYS}D Δ",
                "l_history": st.column_config.LineChartColumn(
                    f"past {N_DAYS}D", width="small"
                ),
            },
            hide_index=True,
        )
