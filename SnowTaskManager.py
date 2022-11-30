# Tested on Python 3.9

#import statement BEGIN
import time
from datetime import datetime
import tzlocal
import pandas as pd
import plotly.graph_objects as go
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

# Variable declaration START
file_dir = os.path.dirname(os.path.dirname(__file__))
task_list_file = (file_dir + '/data/task.csv')
task_hist_file = (file_dir + '/data/task_hist.csv')
current_tz = "'" + tzlocal.get_localzone_name() + "'"
print(tzlocal.get_localzone_name())
print(file_dir)
print(task_list_file)
print(task_hist_file)


