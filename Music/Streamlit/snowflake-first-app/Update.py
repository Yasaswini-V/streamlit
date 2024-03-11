import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col 
import pandas as pd
import streamlit as st
from collections import defaultdict

@st.cache_resource(ttl=600, experimental_allow_widgets=True)
def connect_local():
#st.text ('Local connect')
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
df =pd.DataFrame(con.cursor().execute('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC').fetch_pandas_all())

def get_df():
    df =pd.DataFrame(con.cursor().execute('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC').fetch_pandas_all())
    return df

def get_user():
    df =pd.DataFrame(con.cursor().execute('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.USER_DETAILS').fetch_pandas_all())
    return df

def get_changed_rows(df_old, df_new):
    changed_rows = []
    for index,row in df_new.iterrows(): 
        if not row.equals(df_old.loc[index]):
            changed_rows.append(row)
    return changed_rows

def Update_table(new_df):
    # Convert the Pandas DataFrame to a Snowpark DataFrame
    database='EG_DEV_WRKS_ENGR_DB'
    schema='SHARED_COMMON'
    trg_table_name='TABLE_DETAILS_DESC'
    con.cursor().execute(f"TRUNCATE TABLE {database}.{schema}.{trg_table_name};")
    success, nchunks, nrows, _ = write_pandas(con, new_df, trg_table_name,database,schema)
    return True

def update():
    name=st.text_input("Enter your name")
    df=get_user()
    t_df=get_df()
    data_dict=df.to_dict(orient='list')
    new_df={}
    project_name=[]
    table_data=t_df.to_dict(orient='list')
    for i in table_data.keys():
        new_df[i]=[]
    for i in range(len(data_dict['MEMBER_NAME'])):
        if data_dict['MEMBER_NAME'][i] == name.upper():
            project_name.append(data_dict['PROJECT_NAME'][i])
    for j in project_name:
        for i in range(len(table_data['PROJECT_NAME'])):
            if j==table_data['PROJECT_NAME'][i]:
                for k in table_data.keys():
                    new_df[k].append(table_data[k][i])        
    if 'df' not in st.session_state:
        st.session_state.df=None
    old_df=pd.DataFrame(new_df)
    edited_df= st.data_editor(old_df) 
    changed_rows=get_changed_rows(old_df,edited_df)
# st.write(changed_rows)
    if changed_rows:
        st.write('Changed Rows:')
        st.write((changed_rows))
    else:
        st.write('No changes detected.')


                    

if __name__=='__main__':
    update()