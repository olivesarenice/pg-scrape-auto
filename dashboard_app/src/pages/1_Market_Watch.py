import datetime

import altair as alt
import backend_bq
import numpy as np
import pandas as pd
import streamlit as st

N_DAYS = 18


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
    row_d["psf"] = f"${int(round(row['median_psf'])):,}"
    row_d["psf_delta"] = f"{round(row['psf_pct_delta'] * 100, 1)}%"
    row_d["psf_h"] = row["median_psf_history"].tolist()
    row_d["listing"] = f"{row["listings"]:,}"
    row_d["listing_delta"] = f"{round(row["listings_pct_delta"] * 100, 1)}%"
    row_d["listing_h"] = row["listings_history"].tolist()
    return row_d

def plot_column(col_d, c,n):
    c.markdown(f"**{col_d['description']}**")
    # LISTINGS

    if n == 0:
        l_label = "Listings"
        p_label = "Median PSF"    
    else:
        l_label = ""
        p_label = ""
    #LISTING
    c.metric(l_label, col_d["listing"], col_d["listing_delta"])
    # PSF
    c.metric(p_label, col_d["psf"], col_d["psf_delta"])
    data_psf = pd.DataFrame.from_dict(col_d["psf_h"], orient="columns")
    
    chart_psf = (
        alt.Chart(data_psf)
        .mark_line(point=False)
        .encode(
            x=alt.X("time:T", title=None, axis=alt.Axis(labels=False)),
            y=alt.Y(
                "value:Q",
                title=None,
                scale=alt.Scale(
                    domain=[
                        (data_psf["value"].min() // 100) * 100,
                        ((data_psf["value"].max() // 100) + 1) * 100,
                    ]
                ),
            ),
            tooltip=["time:T", "value:Q"],
        )
        .properties(
            width=100,  # Width of the chart
            height=100,  # Height of the chart to make it square
        )
        .configure_view(stroke=None)  # Remove the border
    )

    # Embed options for PSF chart
    chart_psf["usermeta"] = {
        "embedOptions": {
            "actions": False,
        }
    }

    # Render the PSF chart
    st.altair_chart(chart_psf, use_container_width=True)

    
    # data_listing = pd.DataFrame.from_dict(col_d["listing_h"], orient="columns")
    
    # # Fixing mark_bar() and removing 'point=False'
    # chart_listing = (
    #     alt.Chart(data_listing)
    #     .mark_line(interpolate='step-after')  # Removed invalid `point=False` from mark_bar
    #     .encode(
    #         x=alt.X("time:T", title=None, axis=alt.Axis(labels=False)),
    #         y=alt.Y(
    #             "value:Q",
    #             title=None,
    #             scale=alt.Scale(
    #                 domain=[
    #                     (data_listing["value"].min() // 100) * 100,
    #                     ((data_listing["value"].max() // 100) + 1) * 100,
    #                 ]
    #             ),
    #         ),
    #         tooltip=["time:T", "value:Q"],
    #     )
    #     .properties(
    #         width=100,  # Width of the chart
    #         height=100,  # Height of the chart to make it square
    #     )
    #     .configure_view(stroke=None)  # Remove the border
    # )

    # # Embed options for listing chart
    # # chart_listing["usermeta"] = {
    # #     "embedOptions": {
    # #         "actions": False,
    # #     }
    # # }

    # # Render the listing chart
    # st.altair_chart(chart_listing, use_container_width=True)

df_r = load_data(is_all_region=True)
partition_date = df_r["dt"].max()

print(partition_date)


st.set_page_config(page_title="Market Overview", page_icon="📈")

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
st.markdown("# Market Overview")
st.sidebar.header("Market Overviewo")
st.write(
    """This page shows the median PSF, number of listings, and median lifetime of listings for each category of property."""
)

# Create the HDB container
with st.container():
    st.markdown("## HDB")


    c1, c2, c3, c4, c5 = st.columns(5)
    c_config = [
        {"code": "AH2", "st_c": c1},
        {"code": "AH3", "st_c": c2},
        {"code": "AH4", "st_c": c3},
        {"code": "AH5", "st_c": c4},
        {"code": "AH0", "st_c": c5},
    ]
    for n, c_d in enumerate(c_config):
        c = c_d["st_c"]
        code = c_d["code"]
        print(code)
        print(df_r)
        df_c = df_r[df_r["viz_group_code"] == code]
        col_d = parse_df_row(df_c)
        print(col_d)
        with c:
            plot_column(col_d, c, n)

# Create the NL container
with st.container():
    st.markdown("## Non-Landed")


    c1, c2, c3, c4, c5 = st.columns(5)
    c_config = [
        {"code": "AN1", "st_c": c1},
        {"code": "AN2", "st_c": c2},
        {"code": "AN3", "st_c": c3},
        {"code": "AN4", "st_c": c4},
        {"code": "AN5", "st_c": c5},
    ]
    for n, c_d in enumerate(c_config):
        c = c_d["st_c"]
        code = c_d["code"]
        print(code)
        print(df_r)
        df_c = df_r[df_r["viz_group_code"] == code]
        col_d = parse_df_row(df_c)
        print(col_d)
        with c:
            plot_column(col_d, c,n )

# Create the NL container
with st.container():
    st.markdown("## Landed")


    c1, c2, c3, c4, c5 = st.columns(5)
    c_config = [
        {"code": "AL1", "st_c": c1},
        {"code": "AL2", "st_c": c2},
        {"code": "AL3", "st_c": c3},
    ]
    for n, c_d in enumerate(c_config):
        c = c_d["st_c"]
        code = c_d["code"]
        print(code)
        print(df_r)
        df_c = df_r[df_r["viz_group_code"] == code]
        col_d = parse_df_row(df_c)
        print(col_d)
        with c:
            plot_column(col_d, c, n)
