import snowflake.connector
from PIL import Image
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import streamlit as st
import datetime
from datetime import date
import time
import Main
import conn
 
con=conn.connect_local()

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

def callback_button_save():
    st.session_state.save_form=False
    st.session_state.submit_form=True

def get_df():
    df =pd.DataFrame(con.cursor().execute(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE'").fetch_pandas_all())
    return df 

def insert_check():
    if len(st.session_state.project_name)<=0 or st.session_state.project_name.isnumeric():
        st.toast("❓❓Enter a valid project name")
        callback_button_save()
    elif len(st.session_state.Objects_modified)<=0 or st.session_state.Objects_modified.isnumeric():
        st.toast("❓❓Enter valid Object Modified")
        callback_button_save()
    elif len(st.session_state.Objects_impacted)<=0 or st.session_state.Objects_impacted.isnumeric():
        st.toast("❓❓Enter the valid Objects Impacted")
        callback_button_save()
    elif len(st.session_state.UAT_Env)<=0 or st.session_state.Objects_impacted.isnumeric():
        st.toast("❓❓Enter a valid UAT Environment")
        callback_button_save()
    elif len(st.session_state.G_Changed)<=0:
        st.toast("❓❓Select if the Grain Changed")
        callback_button_save()
    elif len(st.session_state.remarks)<=0:
        st.toast("❓❓Enter remark as none if there are no remarks")
        callback_button_save()
    elif len(st.session_state.primary_contact)<=0:
        st.toast("❓❓Enter the name of primary contact")
        callback_button_save()
    else:
        try:
            con.cursor().execute('INSERT INTO EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC (PROJECT_NAME,OBJECT_MODIFIED,OBJECT_IMPACTED,UAT_ENV,UAT_TIMELINE,DEPLOYMENT_DATE,GRAIN_CHANGED,REMARKS,PRIMARY_CONTACT,DEACTIVATE,INSERTED_ON,UPDATED_ON) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(st.session_state.project_name,st.session_state.Objects_modified,st.session_state.Objects_impacted,st.session_state.UAT_Env,st.session_state.UAT_Timeline,st.session_state.Deployment_date,st.session_state.G_Changed,st.session_state.remarks,st.session_state.primary_contact,'FALSE',st.session_state.inserted_on,'NA'))
            st.session_state.inserted=True
            # Clear the form fields after successful insertion
            fields=['project_name','Objects_modified','Objects_impacted','UAT_Env','UAT_Timeline_from','UAT_Timeline_to','Deployment_date','G_Changed','remarks','primary_contact','inserted_on']
            for key in fields:
                if key=='UAT_Timeline_from' or key == 'UAT_Timeline_to' or key=='Deployment_date':
                    st.session_state[key]=date.today()
                else:
                    st.session_state[key]=""
        except Exception as e:
                st.error(f"Error inserting data: {e}")

def insert():
    im=Image.open('icons/Insert.png')
    resized_image=im.resize((im.height,150))
    col1,_,col2=st.columns([1,0.2,5])
    with col1:
        st.title("Insert")
    with col2:
        st.write(" ")
        image_placeholder = st.empty()
        image_placeholder.image(resized_image, width=60)
    if 'save_form' not in st.session_state:
        st.session_state.save_form=False
    if 'submit_form' not in st.session_state:
        st.session_state.submit_form=True
    if 'inserted' not in st.session_state:
        st.session_state.inserted=False
    
    col1,col2,_=st.columns([1,1,2])
    fields=['project_name','Objects_modified','Objects_impacted','UAT_Env','UAT_Timeline_from','UAT_Timeline_to','Deployment_date','remarks','primary_contact','G_Changed']
    for key in fields:
        if key not in st.session_state:
            if key=='UAT_Timeline_from' or key == 'UAT_Timeline_to' or key=='Deployment_date':
                st.session_state[key]=date.today()
            else:
                st.session_state[key]=""
    if st.session_state.inserted != True:
        with st.form("insertform",clear_on_submit=True): 
            project_name=st.text_input("Enter the project name*",value=st.session_state.project_name).upper()
            Objects_modified=st.text_input("Enter the Objects being Modified*",value=st.session_state.Objects_modified).upper()
            Objects_impacted=st.text_input("Enter the Objects that will be impacted*",value=st.session_state.Objects_impacted).upper()
            UAT_Env=st.text_input("Enter the UAT Environment*",value=st.session_state.UAT_Env).upper()
            st.write("Choose the UAT Timeline")
            col1,buff,col2=st.columns([2,0.5,2])
            UAT_Timeline_from=col1.date_input("From*",value=st.session_state.UAT_Timeline_from)    
            UAT_Timeline_to=col2.date_input("To*",value=st.session_state.UAT_Timeline_to)
            UAT_Timeline=combine_date(st.session_state.UAT_Timeline_from,st.session_state.UAT_Timeline_to)
            Deployment_date=st.date_input("Enter the Deployment_date*",value=st.session_state.Deployment_date)
            G_Changed=st.selectbox("Is the grain changed?*",options=[st.session_state.G_Changed,"YES","NO"])
            remarks=st.text_input("Remarks*",value=st.session_state.remarks).upper()
            primary_contact=st.text_input("Primary contact*",value=st.session_state.primary_contact).upper()
            inserted_on=datetime.datetime.now().replace(microsecond=0)
            if st.session_state.submit_form:
                col1,col2=st.columns(2)
                if col1.form_submit_button('Save'):
                    st.session_state.project_name=project_name
                    st.session_state.Objects_modified=Objects_modified
                    st.session_state.Objects_impacted=Objects_impacted
                    st.session_state.UAT_Env=UAT_Env
                    st.session_state.UAT_Timeline=UAT_Timeline
                    st.session_state.Deployment_date=Deployment_date
                    st.session_state.G_Changed=G_Changed
                    st.session_state.remarks=remarks
                    st.session_state.primary_contact=primary_contact
                    st.session_state.inserted_on=inserted_on        
                    st.info('Your data is saved!! Please click submit to validate and insert data')
                    time.sleep(2)
                    st.rerun()
                col2.form_submit_button("Submit",on_click=insert_check)
                    
            else:
                st.form_submit_button("Save",on_click=callback_button_save())
    if st.session_state.inserted:
        st.success("Inserted")
        st.dataframe(get_df(),hide_index=True,column_config={"ID": None})
        st.session_state.inserted=False
        st.button("Click here to insert new record")

if __name__=='__main__':
    insert()