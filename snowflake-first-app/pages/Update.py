import streamlit as st
from PIL import Image
import io
import base64
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col 
import pandas as pd
import datetime
import Main
from collections import defaultdict
import conn

con=conn.connect_local()

def get_df(opt):
    if opt=='table_details':
        df =pd.DataFrame(con.cursor().execute(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE' AND DEPLOYMENT_DATE>=CURRENT_DATE()").fetch_pandas_all())
        return df
    elif opt=='user_details':
        df =pd.DataFrame(con.cursor().execute('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.USER_DETAILS').fetch_pandas_all())
        return df

def get_changed_rows(df_old, df_new):
    changed_rows = []
    for index,row in df_new.iterrows(): 
        if not row.equals(df_old.loc[index]):
            changed_rows.append(dict(row))
    return changed_rows

def Update_table(values):
    # Convert the Pandas DataFrame to a Snowpark DataFrame
    database='EG_DEV_WRKS_ENGR_DB'
    schema='SHARED_COMMON'
    trg_table_name='TABLE_DETAILS_DESC'
    try:
        for i in values:
            con.cursor().execute(f" UPDATE {database}.{schema}.{trg_table_name} SET PROJECT_NAME=%s,OBJECT_MODIFIED=%s,OBJECT_IMPACTED=%s,UAT_ENV=%s,UAT_TIMELINE=%s,DEPLOYMENT_DATE=%s,GRAIN_CHANGED=%s,REMARKS=%s,PRIMARY_CONTACT=%s WHERE ID=%s;",(i['PROJECT_NAME'],i['OBJECT_MODIFIED'],i['OBJECT_IMPACTED'],i['UAT_ENV'],i['UAT_TIMELINE'],i['DEPLOYMENT_DATE'],i['GRAIN_CHANGED'],i['REMARKS'],i['PRIMARY_CONTACT'],i['ID']))
            con.cursor().execute(f" UPDATE {database}.{schema}.{trg_table_name} SET UPDATED_ON=%s WHERE ID=%s;",(datetime.datetime.now().replace(microsecond=0),i['ID']))
        return True
    except Exception as e:
        st.error(e)

def edit():
    im=Image.open('icons/Update.png')
    img_bytes = io.BytesIO()
    im.save(img_bytes, format="PNG")
    img_bytes = img_bytes.getvalue()
    img_str = base64.b64encode(img_bytes).decode()
    html_code = f"""
            <div style="display: flex; align-items: center;">
                <p style="margin: 0;font-size:40px;font-weight:bold;font-family: 'Agbalumo', serif"> Update  </p>
                <img src="data:image/png;base64,{img_str}" alt="Icon" style="width: 40px; height: 45px; margin-left: 15px;">
            </div>"""

    st.markdown(html_code, unsafe_allow_html=True)
    name=st.text_input("Enter your name")
    df=get_df('user_details')
    t_df=get_df('table_details')
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
    old_df.pop('DEACTIVATE')
    edited_df= st.data_editor(old_df,disabled=["ID","DEACTIVATE","INSERTED_ON","UPDATED_ON"],column_config={"ID": None}) 
    col1,col2=st.columns(2)
    col1.button("Refresh")
    submit=col2.button("Submit")
    if submit:
        changed_rows=get_changed_rows(old_df,edited_df)
        if changed_rows:
            for i in changed_rows:
                for key,value in i.items():
                    if key != 'ID' and key!='UAT_TIMELINE' and key!='DEPLOYMENT_DATE':
                        i[key]=value.upper()
        update=Update_table(changed_rows)
        if update: 
            st.success('Modified')

if __name__=='__main__':
    edit()