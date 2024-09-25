import datetime
import json

import altair as alt
import backend_bq
import numpy as np
import pandas as pd
import streamlit as st

N_DAYS = 21



def format_q(n):
    if n >= 10_000_000:
        return f"$ {n / 1_000_000:,.1f}".replace(",", ".") + "M"
    elif n >= 1_000_000:
        return f"$ {n / 1_000_000:,.2f}".replace(",", ".") + "M"
    elif n >= 1_000:
        return f"$ {int(n // 1_000)}K"
    else:
        return str(n)

def parse_df_row(row: pd.DataFrame) -> dict:
    if len(row) != 1:
        return TypeError
    row = row.iloc[0]
    row_d = {}
    row_d["description"] = row["description"].split(" | ")[1]
    row_d["q"] = format_q(row['median_q'])
    row_d["q_delta"] = f"{round(row['q_pct_delta'] * 100, 1)}%"
    row_d["q_h"] = row["median_q_history"].tolist()
    row_d["psf"] = f"$ {int(round(row['median_psf'])):,}"
    row_d["psf_delta"] = f"{round(row['psf_pct_delta'] * 100, 1)}%"
    row_d["psf_h"] = row["median_psf_history"].tolist()
    row_d["listing"] = f"{row["listings"]:,}"
    row_d["listing_delta"] = f"{round(row["listings_pct_delta"] * 100, 1)}%"
    row_d["listing_h"] = row["listings_history"].tolist()
    row_d["lifetime"] = f"{round(row["median_lifetime"])} day"
    return row_d



def plot_column(col_d, c,n):
    c.markdown(f"**{col_d['description']}**")
    # LISTINGS

    if n == 0:
        l_label = "Active listings"
        p_label = "Median PSF"
        q_label = "Median Quantum"    
    else:
        l_label = ""
        p_label = ""
        q_label = ""
    #LISTING
    c.markdown(f"{col_d['listing']} listings   \n:grey[{col_d['lifetime']} lifetime]")
    # Q
    c.metric(q_label, col_d["q"], col_d["q_delta"])
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
st.sidebar.header("Market Overview")
st.write(
    """This page shows the median PSF, number of listings, and median lifetime of listings for each category of property."""
)

SEGMENT = st.radio(
    f"Property Type",
    ["HDB", "Non-Landed", "Landed"],
    captions=[
        "Resale flats",
        "Condominium, Apartments, ECs",
        "Terrace, Semi-Detached, Detached, Bungalows",
    ], horizontal = True
)

b1, b2 = st.columns(2)

with b1:
    n_weeks = st.select_slider(
    "Past weeks",
    options=[n for n in range(1,4)])
    N_DAYS = n_weeks * 7




df_r = backend_bq.load_data(N_DAYS,is_all_region=True)
partition_date = df_r["dt"].max()

# print(partition_date)

match SEGMENT:
    case "HDB":
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
                # print(code)
                # print(df_r)
                df_c = df_r[df_r["viz_group_code"] == code]
                col_d = parse_df_row(df_c)
                # print(col_d)
                with c:
                    plot_column(col_d, c, n)

    case "Non-Landed":
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
                # print(code)
                # print(df_r)
                df_c = df_r[df_r["viz_group_code"] == code]
                col_d = parse_df_row(df_c)
                # print(col_d)
                with c:
                    plot_column(col_d, c,n )

    case "Landed":
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
                # print(code)
                # print(df_r)
                df_c = df_r[df_r["viz_group_code"] == code]
                col_d = parse_df_row(df_c)
                # print(col_d)
                with c:
                    plot_column(col_d, c, n)
