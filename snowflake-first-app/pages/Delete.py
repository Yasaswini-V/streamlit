import snowflake.connector
from PIL import Image
import io
import base64
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import streamlit as st
import datetime
from datetime import date 
import Main
import conn

con=conn.connect_local()

def Get_rows(df):
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Delete", False)
    df_with_selections.pop('DEACTIVATE')
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Delete": st.column_config.CheckboxColumn(required=True),"ID": None},
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

def get_df(opt):
    if opt=='table_details':
        df =pd.DataFrame(con.cursor().execute(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE' AND DEPLOYMENT_DATE>=CURRENT_DATE()").fetch_pandas_all())
        return df
    elif opt=='user_details':
        df =pd.DataFrame(con.cursor().execute('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.USER_DETAILS').fetch_pandas_all())
        return df

def delete():
    # st.set_page_config(page_title="Delete", page_icon="📈")
    im=Image.open('icons/Delete.png')
    img_bytes = io.BytesIO()
    im.save(img_bytes, format="PNG")
    img_bytes = img_bytes.getvalue()

    # Encode bytes as base64
    img_str = base64.b64encode(img_bytes).decode()

    # Define HTML code with image and text
    html_code = f"""
            <div style="display: flex; align-items: center;">
                <p style="margin: 0;font-size:40px;font-weight:bold;font-family: 'Agbalumo', serif"> Delete  </p>
                <img src="data:image/png;base64,{img_str}" alt="Icon" style="width: 50px; height: 45px; margin-left: 10px;">
            </div>"""

    st.markdown(html_code, unsafe_allow_html=True)
    st.write("")
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
    old_df=pd.DataFrame(new_df)
    selection = list(Get_rows(old_df))
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
            df=get_df('table_details')
            st.dataframe(df,hide_index=True,column_config={"ID": None})

if __name__=='__main__':
        delete()