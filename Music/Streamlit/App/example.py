import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import numpy as np
import time

if "data" not in st.session_state:
    st.session_state.data = np.random.randn(10, 20)
    st.session_state.data3 = np.random.randn(20, 3)
    st.session_state.mapdata = np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4]
data = st.session_state.data
data3 = st.session_state.data3
mapdata = st.session_state.mapdata

df = pd.DataFrame({
    'first column': [1, 2, 3, 4],
    'second column': [10, 20, 30, 40]
})
df

st.write("Here's our first attempt at using data to create a table:")
st.write(pd.DataFrame({
    'first column': [1, 2, 3, 4],
    'second column': [10, 20, 30, 40]
}))

dataframe = data
st.dataframe(dataframe)

dataframe = pd.DataFrame(
    data,
    columns=('col %d' % i for i in range(20))
)

st.dataframe(dataframe.style.highlight_max(axis=0))

dataframe = pd.DataFrame(
    data,
    columns=('col %d' % i for i in range(20))
)
st.table(dataframe)

chart_data = pd.DataFrame(
    data3,
    columns=['a', 'b', 'c']
)

st.line_chart(chart_data)

map_data = pd.DataFrame(
    mapdata,
    columns=['lat', 'lon']
)

st.map(map_data)

x = st.slider('x') # :point_left: this is a widget
st.write(x, 'squared is', x * x)