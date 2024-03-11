import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import streamlit as st
import datetime
from datetime import date
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

def combine_date(date1,date2):
    date=''
    dd1=date1.strftime('%d/%m/%y')
    dd2=date2.strftime('%d/%m/%y')
    dd1,mm1,yy1=dd1.split("/")
    dd2,mm2,yy2=dd2.split("/")
    if date1.strftime('%y')==date2.strftime('%y'):
        date=date1.strftime('%d/%m')+'-'+date2.strftime('%d/%m/%y')
    else:
        date=date1.strftime('%d/%m/%y')+'-'+date2.strftime('%d/%m/%y')
    return date

def Get_rows(df):
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Delete", False)
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Delete": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
    )
    rows = edited_df[edited_df.Delete].index
    return rows

def get_df(opt):
    if opt=='table_details':
        df =pd.DataFrame(con.cursor().execute('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC').fetch_pandas_all())
        return df
    elif opt=='user_details':
        df =pd.DataFrame(con.cursor().execute('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.USER_DETAILS').fetch_pandas_all())
        return df

def color_coding(row):
    return ['background-color:yellow'] * len(row) if row.GRAIN_CHANGED == 'YES' else [''] * len(row)

def Update_table(new_df):
    # Convert the Pandas DataFrame to a Snowpark DataFrame
    database='EG_DEV_WRKS_ENGR_DB'
    schema='SHARED_COMMON'
    trg_table_name='TABLE_DETAILS_DESC'
    con.cursor().execute(f"TRUNCATE TABLE {database}.{schema}.{trg_table_name};")
    success, nchunks, nrows, _ = write_pandas(con, new_df, trg_table_name,database,schema)
    return True

# def check_len(fields):
#     for field,value in fields.items():
#         if len(field)<=0:
#             return (field,False)
        
def insert():
    fields=['project_name','Objects_modified','Objects_impacted','UAT_Env','UAT_Timeline_from','UAT_Timeline_to','Deployment_date','remarks','primary_contact']
    for key in fields:
        if key not in st.session_state:
            if key=='UAT_Timeline_from' or key == 'UAT_Timeline_to' or key=='Deployment_date':
                st.session_state[key]=date.today()
            else:
                st.session_state[key]=""
    
    with st.form("insertform",clear_on_submit=True): # Set clear_on_submit to False
        st.session_state.project_name=st.text_input("Enter the project name*",value=st.session_state.project_name).upper()
        st.session_state.Objects_modified=st.text_input("Enter the Objects being Modified*",value=st.session_state.Objects_modified).upper()
        st.session_state.Objects_impacted=st.text_input("Enter the Objects that will be impacted*",value=st.session_state.Objects_impacted).upper()
        st.session_state.UAT_Env=st.text_input("Enter the Testing Environment*",value=st.session_state.UAT_Env).upper()
        st.write("Choose the UAT Timeline")
        col1,buff,col2=st.columns([2,0.5,2])
        st.session_state.UAT_Timeline_from=col1.date_input("From*",value=st.session_state.UAT_Timeline_from)    
        st.session_state.UAT_Timeline_to=col2.date_input("To*",value=st.session_state.UAT_Timeline_to)
        UAT_Timeline=combine_date(st.session_state.UAT_Timeline_from,st.session_state.UAT_Timeline_to)
        st.session_state.Deployment_date=st.date_input("Enter the Deployment_date*",value=st.session_state.Deployment_date)
        G_Changed=st.selectbox("Is the grain changed?*",options=["","YES","NO"])
        st.session_state.remarks=st.text_input("Remarks*",value=st.session_state.remarks).upper()
        st.session_state.primary_contact=st.text_input("Primary contact*",value=st.session_state.primary_contact).upper()
        # st.write(st.session_state)
        btn_clk=st.form_submit_button("Submit")
        if (btn_clk):
            if len(st.session_state.project_name)<=0:
                st.error("Enter the project name")
            elif len(st.session_state.Objects_modified)<=0:
                st.error("Enter the Object Modified")
            elif len(st.session_state.Objects_impacted)<=0:
                st.error("Enter the Objects Impacted")
            elif len(st.session_state.UAT_Env)<=0:
                st.error("Enter the UAT Environment")
            elif len(G_Changed)<=0:
                st.error("Select if the Grain Changed")
            elif len(st.session_state.primary_contact)<=0:
                st.error("Enter the name of primary contact")
            else:
                try:
                    con.cursor().execute('INSERT INTO EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC (PROJECT_NAME,OBJECT_MODIFIED,OBJECT_IMPACTED,UAT_ENV,UAT_TIMELINE,DEPLOYMENT_DATE,GRAIN_CHANGED,REMARKS,PRIMARY_CONTACT) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',(st.session_state.project_name,st.session_state.Objects_modified,st.session_state.Objects_impacted,st.session_state.UAT_Env,UAT_Timeline,st.session_state.Deployment_date,G_Changed,st.session_state.remarks,st.session_state.primary_contact))
                    st.success("Inserted")
                    st.dataframe(get_df('table_details'),hide_index=True)
                    # Clear the form fields after successful insertion
                    for key in fields:
                        if key=='UAT_Timeline_from' or key == 'UAT_Timeline_to' or key=='Deployment_date':
                            st.session_state[key]=date.today()
                        else:
                            st.session_state[key]=""
                    st.write(st.session_state)
                except Exception as e:
                    st.error(f"Error inserting data: {e}")



def delete():
    df=get_df('table_details')
    selection = list(Get_rows(df))
    sub,wt,buff=st.columns([1,1,2])
    submit=sub.button('Delete')
    removed=[]
    if submit:
        for i in selection:
            removed.append(df.loc[i]['PROJECT_NAME'])
            df=df.drop(i)
        if Update_table(df):
            if len(removed)>1:
                st.success(f"Rows with project names {','.join(removed)} dropped")
            else:
                st.success(f"Row with project name {''.join(removed)} dropped")
            st.write("Your selection:")
            st.dataframe(df,hide_index=True)

def set_save_button():
    st.session_state.save_button=not st.session_state.save_button
# @st.cache
def edit():
    if 'save_button' not in st.session_state:
        st.session_state.save_button=False
    if 'df' not in st.session_state:
        st.session_state.df=None
    st.session_state.df=get_df('table_details')
    # df.insert(0,'Update',False)
    st.session_state.df=st.data_editor(st.session_state.df,hide_index=True)
    st.write(st.session_state.save_button)
    save=st.button('Save',on_click=set_save_button)
    if st.session_state.save_button:
        Update_table(st.session_state.df)
        st.success('Row Updated')
        st.session_state.save_button=False
        st.write('Edited Table:')
        st.dataframe(df,hide_index=True)


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
        edit()
    if (Delete_button or st.session_state.Delete_button):
        delete()

def main():
    if 'option' not in st.session_state:
            st.session_state.option=None
    option=st.selectbox("Choose the appropriate action you want to perform",options=["","Update","Insert","Display"])
    
    if option=='Insert':
        st.session_state.option='Insert'
        insert()

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
        

    
if __name__=="__main__":
    main()
