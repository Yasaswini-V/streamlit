import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import streamlit as st
import datetime
from datetime import date 
import main

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

def Get_rows(df):
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Delete", False)
    df_with_selections.pop('DEACTIVATE')
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Delete": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
    )
    rows = edited_df[edited_df.Delete].index
    return rows

def Delete_table(rows):
    # Convert the Pandas DataFrame to a Snowpark DataFrame
    database='EG_DEV_WRKS_ENGR_DB'
    schema='SHARED_COMMON'
    trg_table_name='TABLE_DETAILS_DESC'
    for i in range(len(rows)):
        con.cursor().execute(f" UPDATE {database}.{schema}.{trg_table_name} SET DEACTIVATE=%s WHERE ID=%s;",('TRUE',rows[i]))
    return True

def update_df():
    df =pd.DataFrame(con.cursor().execute(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE'").fetch_pandas_all())
    return df

def delete():
    df=update_df()
    selection = list(Get_rows(df))
    removed=[]
    project=[]
    col1,col2=st.columns(2)
    col1.button("Refresh")
    submit=col2.button('Delete')
    if submit:
        for i in selection:
            removed.append(df.loc[i]['ID'])
            project.append(df.loc[i]['PROJECT_NAME'])
        #     df=df.drop(i)
        if Delete_table(removed):
            if len(removed)>1:
                st.success(f"Rows with project names {','.join(project)} dropped")
            else:
                st.success(f"Row with project name {''.join(project)} dropped")
            st.write("Your selection:")
            df=update_df()
            st.dataframe(df,hide_index=True)

if __name__=='__main__':
        delete()