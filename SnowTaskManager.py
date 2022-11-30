# Tested on Python 3.9

#import statement BEGIN
import time
from datetime import datetime
import tzlocal
import pandas as pd
import plotly.graph.objects as go
import snowflake.connector
import streamlit as st
from plotly.subplots import make_subplots
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import os

#Import statement END

# setting page config- START
st.set_page_config(layout="wide")
st.markdown(
    "<h1 style='text-align: center; color: #1489a6;'>❄❄ SN❄WFLAKE TASK MANAGER ❄❄</h1>",
    unsafe_allow_html=True)

# page configurtion end


# Css https://docs.streamlit.io/knowledge-base/using-streamlit/hide-row-indices-displaying-dataframe
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """
st.markdown(hide_table_row_index, unsafe_allow_html=True)

# setting page config- END
