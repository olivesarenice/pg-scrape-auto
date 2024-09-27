import datetime

import altair as alt
import backend_bq
import matplotlib
import numpy as np
import pandas as pd
import streamlit as st

N_DAYS = 21
SEGMENT = "HDB"


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
st.markdown(f"# Listings Stats by Region & Size")
st.sidebar.header("Listing Stats by Region & Size")
st.write(
    """This page shows the median PSF, number of listings, and median lifetime of listings for each HDB type, split by regions"""
)



st.session_state.disabled = False
SEGMENT = st.radio(
    f"Property Type",
    ["HDB", "Non-Landed", "Landed"],
    captions=[
        "Resale flats",
        "Condominium, Apartments, ECs",
        "Terrace, Semi-Detached, Detached, Bungalows",
    ],
    horizontal=True
)
b1, b2 = st.columns(2)
with b1:
    metric = st.radio(
    f"Metric Comparison",
    ["Absolute", f"% change"],
    horizontal=True
)
    if metric == 'Absolute':
        st.session_state.disabled = True
with b2:
    n_weeks = st.select_slider(
    "Past weeks (only for % change)",
    options=[n for n in range(1,4)],
    disabled=st.session_state.disabled)
    N_DAYS = n_weeks * 7



df_r = backend_bq.load_data(N_DAYS,is_all_region=False)
partition_date = df_r["dt"].max()

# print(partition_date)

df_r.to_csv("sample_data.csv")
# print(df_r)

df_r["median_psf"] = df_r["median_psf"].apply(round)
df_r["description"] = df_r["description"].apply(lambda x: x.split(" | ")[1])
df_r["q_pct_delta"] = df_r["q_pct_delta"].apply(lambda x: round(x*100,2))
df_r["psf_pct_delta"] = df_r["psf_pct_delta"].apply(lambda x: round(x*100,2))
df_r["listings_pct_delta"] = df_r["listings_pct_delta"].apply(lambda x: round(x*100,2))
df_r["p_history"] = df_r["median_psf_history"].apply(lambda x: [i["value"] for i in list(x)])
df_r["l_history"] = df_r["listings_history"].apply(lambda x: [i["value"] for i in list(x)])



# for HDB

segment_code_map = {
    "HDB": ["AH2","AH3","AH4","AH5","AH0"],
    "Non-Landed": ["AN1","AN2","AN3","AN4","AN5"],
    "Landed": ["AL1","AL2","AL3"],
}


# for segment, types in segment_code_map.items():
segment = SEGMENT
types = segment_code_map[SEGMENT]
df_segment = df_r[df_r["viz_group_code"].isin(types)]

if metric == 'Absolute':

    st.markdown("## Median Quantum")
    df_piv = df_segment.pivot_table(index=["region_code", "region"], columns='description',values="median_q")
    df_piv = df_piv.reset_index(level='region_code',drop=True)
    df_piv.rename_axis('Region', axis='index', inplace=True)
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap='bone', subset=df_piv.columns,vmin=min_value, vmax=max_value*2).format(precision=0, thousands=",")
    st.dataframe(df_styled, use_container_width=True)

    st.markdown("## Median PSF")
    df_piv = df_segment.pivot_table(index=["region_code", "region"], columns='description',values="median_psf")
    df_piv = df_piv.reset_index(level='region_code',drop=True)
    df_piv.rename_axis('Region', axis='index', inplace=True)
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap='bone', subset=df_piv.columns,vmin=min_value, vmax=max_value*2).format(precision=0, thousands=",")
    st.dataframe(df_styled, use_container_width=True)

    st.markdown("## Number of listings")
    df_piv = df_segment.pivot_table(index=["region_code", "region"], columns='description',values="listings")
    df_piv = df_piv.reset_index(level='region_code',drop=True)
    df_piv.rename_axis('Region', axis='index', inplace=True)
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap='bone', subset=df_piv.columns,vmin=min_value, vmax=max_value*2).format(precision=0, thousands=",")
    st.dataframe(df_styled, use_container_width=True)

else:
    from matplotlib.colors import LinearSegmentedColormap

    color_min    = "#ac0a38"
    color_center = "black"
    color_max    = "#0e7c08"
    my_cmap = LinearSegmentedColormap.from_list(
        "my_cmap",
        [color_min, color_center, color_max]
    )
    
    st.markdown(f"## Median Quantum")
    st.markdown(f":grey[{N_DAYS} day % change]")
    df_piv = df_segment.pivot_table(index=["region_code", "region"], columns='description',values="q_pct_delta")
    df_piv = df_piv.reset_index(level='region_code',drop=True)
    df_piv.rename_axis('Region', axis='index', inplace=True)
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap=my_cmap, subset=df_piv.columns,vmin=-5, vmax=5).format(precision=1)
    st.dataframe(df_styled, use_container_width=True)

    st.markdown(f"## Median PSF")
    st.markdown(f":grey[{N_DAYS} day % change]")
    df_piv = df_segment.pivot_table(index=["region_code", "region"], columns='description',values="psf_pct_delta")
    df_piv = df_piv.reset_index(level='region_code',drop=True)
    df_piv.rename_axis('Region', axis='index', inplace=True)
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap=my_cmap, subset=df_piv.columns,vmin=-5, vmax=5).format(precision=1)
    st.dataframe(df_styled, use_container_width=True)

    st.markdown(f"## Number of listings")
    st.markdown(f":grey[{N_DAYS} day % change]")
    df_piv = df_segment.pivot_table(index=["region_code", "region"], columns='description',values="listings_pct_delta")
    df_piv = df_piv.reset_index(level='region_code',drop=True)
    df_piv.rename_axis('Region', axis='index', inplace=True)
    max_value = df_piv.max().max()
    min_value = df_piv.min().min()
    df_styled = df_piv.style.background_gradient(cmap=my_cmap, subset=df_piv.columns,vmin=-20, vmax=20).format(precision=1)
    st.dataframe(df_styled, use_container_width=True)

