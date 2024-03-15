import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from datetime import date
import streamlit as st
import Update as Update
import Insert as insert
import Delete as delete

@st.cache_resource(ttl=6000, experimental_allow_widgets=True) 
def connect_local():
    conn = snowflake.connector.connect(
    account='ecolab.east-us-2.azure',
    user='YASASWINI.V@ECOLAB.COM',
    warehouse='EG_DEV_SVC_ETL_ENGR_WH',
    role='EG_DEV_ENGR_FR',
    database='EG_DEV_WRKS_ENGR_DB',
    schema='SHARED_COMMON',
    authenticator='externalbrowser',
    client_session_keep_alive = 'true'
    )
    return conn
 
con=connect_local()

def get_df(opt):
    if opt=='table_details':
        df =pd.DataFrame(con.cursor().execute(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE'").fetch_pandas_all())
        return df

def color_coding(row):
    if row.DEPLOYMENT_DATE < date.today() and row.GRAIN_CHANGED=='YES':
        return ['background-color:#abc4ff'] * len(row)
    elif row.DEPLOYMENT_DATE < date.today() and row.GRAIN_CHANGED=='NO':
        return ['background-color:#c1d3fe '] * len(row) 
    elif row.GRAIN_CHANGED == 'YES':
        return ['background-color:#e2eafc'] * len(row) 
    else:
        return [''] * len(row)

def update():
    col1,col2,_=st.columns([1,1,2])
    if 'Delete_button' not in st.session_state:
        st.session_state.Delete_button=False
    if 'edit_button' not in st.session_state:
        st.session_state.edit_button=False
    def del_callback():
        st.session_state.Delete_button=True
        st.session_state.edit_button=False
    def edit_callback():
        st.session_state.edit_button=True
        st.session_state.Delete_button=False
    edit_button=col1.button("Edit Table",on_click=edit_callback,key='edit')
    Delete_button=col2.button("Delete Row",on_click=del_callback,key='delete')
    if (edit_button or st.session_state.edit_button):
        Update.edit()
    if (Delete_button or st.session_state.Delete_button):
        delete.delete()

def main():
    if 'option' not in st.session_state:
            st.session_state.option=None
    option=st.selectbox("Choose the appropriate action you want to perform",options=["","Update","Insert","Display"])
    
    if option=='Insert':
        st.session_state.option='Insert'
        insert.insert()

    elif option=='Update':
        if option!=st.session_state.option:
            st.session_state.edit_button=False
            st.session_state.Delete_button=False
        st.session_state.option='Update'
        update()
    elif option=='Display':
        st.session_state.option='Display'
        df=get_df('table_details')
        styled_df=df.style.apply(color_coding,axis=1)
        st.dataframe(styled_df,hide_index=True)
        st.markdown("""
            <div style="display: flex; align-items: center;">
                <div style="width: 20px; height: 20px; border-radius: 50%;background-color:#e2eafc; border: 2px solid black; margin-right: 10px;"></div>
                <p style="margin: 0;">Grain is changed</p>
            </div>
            <div style="display: flex; align-items: center;">
                <div style="width: 20px; height: 20px; border-radius: 50%;background-color:#c1d3fe; border: 2px solid black; margin-right: 10px;"></div>
                <p style="margin: 0;">Deplayment date passed, Grain is not changed</p>
            </div>
            <div style="display: flex; align-items: center;">
                <div style="width: 20px; height: 20px; border-radius: 50%;background-color:#abc4ff; border: 2px solid black; margin-right: 10px;"></div>
                <p style="margin: 0;">Deployment date passed, Grain is changed.</p>
            </div>
            """, unsafe_allow_html=True)
        # st.markdown("""<html>
        #                 <head>
        #                 <style>
        #                 .dot {
        #                 height: 25px;
        #                 width: 25px;
        #                 background-color: #c9dcff;
        #                 border-radius: 50%;
        #                 display: inline-block;
        #                 }
        #                 </style>
        #                 </head>
        #                 <body>
        #                 <div style="text-align:left">
        #                 <span class="dot"></span>
        #                 </div>
        #                 </body>
        #                 </html> """, unsafe_allow_html=True)
       

    
if __name__=="__main__":
    main()
