# https://docs.streamlit.io/get-started/tutorials/create-a-multipage-app

import streamlit as st

st.set_page_config(
    page_title="Welcome!",
    page_icon="👋",
)

st.write("# Welcome to Streamlit! 👋")

st.sidebar.success("Select a demo above.")

st.markdown(
    """
    This app shows the metrics of listings on PropertyGuru website. Data on all 50,000+ listings is collected daily and displayed here.
    
    Today's raw listing data is can be downloaded [here](https://www.oliverq.site/external/public_datasets/pg-scrape-auto/latest_daily.csv).
    
    Do drop me a message @ [oliverqsw@gmail.com](mailto:oliverqsw@gmail.com) if[oliverqsw@gmail.com you would like the entire set of daily listings starting from 03 Sep 2024.
"""
)
