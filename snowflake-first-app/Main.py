import snowflake.connector
from PIL import Image
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from datetime import date
import streamlit as st
import conn


con=conn.connect_local()

def get_df(opt):
    if opt=='table_details':
        df =pd.DataFrame(con.cursor().execute(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE'").fetch_pandas_all())
        return df

def color_coding(row):
    if row.DEPLOYMENT_DATE < date.today() and row.GRAIN_CHANGED=='YES':
        return ['background-color:#93AFED'] * len(row)
    elif row.DEPLOYMENT_DATE < date.today() and row.GRAIN_CHANGED=="NO":
        return ['background-color:#B7BEFC'] * len(row) 
    else:
        return [''] * len(row)

def main():
    im=Image.open('icons/Table.png')
    col1,_,col2=st.columns([1,0.2,4])
    with col1:
        st.title("Display")
    with col2:
        st.write(" ")
        image_placeholder = st.empty()
        image_placeholder.image(im, width=50)
    df=get_df('table_details')
    styled_df=df.style.apply(color_coding,axis=1)
    st.dataframe(styled_df,hide_index=True,column_config={"ID": None,"DEACTIVATE":None})
    part1,part2,_=st.columns([0.25,2,3])
    with part1:
        st.write("")
        im1=Image.open('icons/icon1.png')
        st.image(im1)
    with part2:
        st.markdown("**Deployment date passed and Grain not changed**")
    row1,row2,_=st.columns([0.25,2,3])
    with row1:
        st.write("")
        im1=Image.open('icons/icon3.png')
        st.image(im1)
    with row2:
        st.markdown("**Deployment date passed and Grain changed**")
        
if __name__=="__main__":
    main()
