import streamlit as st
from PIL import Image
from snowflake.snowpark.context import get_active_session
import pandas as pd
import datetime
from datetime import date 
from st_on_hover_tabs import on_hover_tabs
from collections import defaultdict
import time


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

def get_df(opt):
    session=get_active_session()
    if opt=='table_details':
        df =pd.DataFrame(session.sql(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE' AND DEPLOYMENT_DATE>=CURRENT_DATE()").collect())
        return df
    elif opt=='full_table':
        df =pd.DataFrame(session.sql(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE'").collect())
        return df
    elif opt=='user_details':
        df =pd.DataFrame(session.sql('SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.USER_DETAILS').collect())
        return df 

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

def get_changed_rows(df_old, df_new):
    changed_rows = []
    for index,row in df_new.iterrows(): 
        if not row.equals(df_old.loc[index]):
            changed_rows.append(dict(row))
    return changed_rows

def Update_table(rows,opt):
    session=get_active_session()
    if opt=='Delete':
        for i in range(len(rows)):
            try:
                # st.write(rows)
                out = session.sql(f"""UPDATE EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC SET DEACTIVATE='TRUE' WHERE ID ={rows[i]}""").collect()
                return True
            except Exception as e:
                st.error(e)

    elif opt=='Update':
        try:
            for i in rows:
                # st.write(i)
                out=session.sql(f"""UPDATE EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC SET PROJECT_NAME='{i['PROJECT_NAME']}',OBJECT_MODIFIED='{i['OBJECT_MODIFIED']}',OBJECT_IMPACTED='{i['OBJECT_IMPACTED']}',UAT_ENV='{i['UAT_ENV']}',UAT_TIMELINE='{i['UAT_TIMELINE']}',DEPLOYMENT_DATE='{i['DEPLOYMENT_DATE']}',GRAIN_CHANGED='{i['GRAIN_CHANGED']}',REMARKS='{i['REMARKS']}',PRIMARY_CONTACT='{i['PRIMARY_CONTACT']}' WHERE ID='{i['ID']}'""").collect()
                id=session.sql(f"""UPDATE EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC SET UPDATED_ON='{datetime.datetime.now().replace(microsecond=0)}' WHERE ID='{i['ID']}'""").collect()
                return True
        except Exception as e:
            st.error(e)

def color_coding(row):
    if row.DEPLOYMENT_DATE < date.today():
        return ['background-color:#93e1d8'] * len(row)
    elif row.GRAIN_CHANGED=="YES":
        return ['background-color:#ffa69e'] * len(row) 
    else:
        return [''] * len(row)

def edit():
    im=Image.open('icons/Update.png')
    col1,_,col2=st.columns([7,0.1,4])
    with col1:
        st.subheader("Update an Existing Project Entry")
    with col2:
        # st.write(" ")
        image_placeholder = st.empty()
        image_placeholder.image(im, width=55)
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
    edited_df= st.data_editor(old_df,disabled=["INSERTED_ON","UPDATED_ON"],column_config={"ID":None,"DEACTIVATE":None}) 
    col1,col2=st.columns(2)
    col1.button("Refresh")
    submit=col2.button("Submit")
    if submit:
        changed_rows=get_changed_rows(old_df,edited_df)
        if changed_rows:
            for i in changed_rows:
                for key,value in i.items():
                    if key != 'ID' and key!='UAT_TIMELINE' and key!='DEPLOYMENT_DATE':
                        # st.write(key,value)
                        i[key]=value.upper()
        # save=st.button("Save")
        update=Update_table(changed_rows,"Update")
        if update: 
            st.success('Modified')

def delete():
    im=Image.open('icons/Delete.png')
    resized_image=im.resize((im.height,150))
    col1,_,col2=st.columns([1,0.2,5])
    with col1:
        st.title("Delete")
    with col2:
        st.write(" ")
        image_placeholder = st.empty()
        image_placeholder.image(resized_image, width=60)
    name=st.text_input("Enter your name",key='delete')
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
            removed.append(old_df.loc[i]['ID'])
            project.append(old_df.loc[i]['PROJECT_NAME'])
        if Update_table(removed,"Delete"):
            if len(removed)>1:
                st.success(f"Rows with project names {','.join(project)} dropped")
            else:
                st.success(f"Row with project name {''.join(project)} dropped")

def insert_check():
    if len(st.session_state.project_name)<=0:
        st.toast("❓❓Enter the project name")
        callback_button_save()
    elif len(st.session_state.Objects_modified)<=0:
        st.toast("❓❓Enter the Object Modified")
        callback_button_save()
    elif len(st.session_state.Objects_impacted)<=0:
        st.toast("❓❓Enter the Objects Impacted")
        callback_button_save()
    elif len(st.session_state.UAT_Env)<=0:
        st.toast("❓❓Enter the UAT Environment")
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
        session=get_active_session()
        try:
            out=session.sql(f""" INSERT INTO EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC (PROJECT_NAME, OBJECT_MODIFIED, OBJECT_IMPACTED, UAT_ENV, UAT_TIMELINE, DEPLOYMENT_DATE, GRAIN_CHANGED, REMARKS, PRIMARY_CONTACT, DEACTIVATE, INSERTED_ON, UPDATED_ON) VALUES ('{st.session_state.project_name}','{st.session_state.Objects_modified}','{st.session_state.Objects_impacted}','{st.session_state.UAT_Env}','{st.session_state.UAT_Timeline}','{st.session_state.Deployment_date}','{st.session_state.G_Changed}','{st.session_state.remarks}','{st.session_state.primary_contact}','FALSE','{st.session_state.inserted_on}','NA') """).collect()
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
    col1,_,col2=st.columns([7,0.1,5])
    with col1:
        st.subheader("Register a New Project Entry")
    with col2:
        # st.write(" ")
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
            project_name=st.text_input("Enter the project name*",value=st.session_state.project_name, help="Specify the project name which is going into deployment").upper()
            Objects_modified=st.text_input("Enter the Objects being Modified*",value=st.session_state.Objects_modified, help="Specify the objects that are being modified").upper()
            Objects_impacted=st.text_input("Enter the Objects that will be impacted*",value=st.session_state.Objects_impacted, help="If more than one object is impacted enter it with objects seperated by comma(,)").upper()
            UAT_Env=st.text_input("Enter the UAT Database and Schema*",value=st.session_state.UAT_Env, help="Enter the UAT environment in the format database.schema").upper()
            st.write("Choose the UAT Timeline")
            col1,buff,col2=st.columns([2,0.5,2])
            UAT_Timeline_from=col1.date_input("From*",value=st.session_state.UAT_Timeline_from,help="Enter the date when the UAT will be started")    
            UAT_Timeline_to=col2.date_input("To*",value=st.session_state.UAT_Timeline_to,help="Enter the date when the UAT will be ended")
            UAT_Timeline=combine_date(st.session_state.UAT_Timeline_from,st.session_state.UAT_Timeline_to)
            Deployment_date=st.date_input("Enter the Deployment_date*",value=st.session_state.Deployment_date)
            G_Changed=st.selectbox("Is the grain changed?*",options=[st.session_state.G_Changed,"YES","NO"])
            remarks=st.text_input("Remarks*",value=st.session_state.remarks).upper()
            primary_contact=st.text_input("Primary contact*",value=st.session_state.primary_contact,help="Enter the email id of the primary contact").upper()
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
                    st.toast('Your data is saved!! Please click submit to validate and insert data')
                    time.sleep(2)
                    st.rerun()
                col2.form_submit_button("Submit",on_click=insert_check)
                    
            else:
                st.form_submit_button("Save",on_click=callback_button_save())
    if st.session_state.inserted:
        st.success("Inserted")
        st.dataframe(get_df("full_table"),hide_index=True,column_config={"ID":None,"DEACTIVATE":None})
        st.session_state.inserted=False
        st.button("Click here to insert new record")

def display():
    im=Image.open('icons/Table.png')
    col1,_,col2=st.columns([8,0.1,15])
    with col1:
        st.subheader("Registered Projects List for Testing")
    with col2:
        image_placeholder = st.empty()
        image_placeholder.image(im, width=50)
    df=get_df("full_table")
    styled_df=df.style.apply(color_coding,axis=1)
    st.dataframe(styled_df,hide_index=True,column_config={"ID": None,"DEACTIVATE":None})
    part1,part2,_=st.columns([0.25,2,3])
    with part1:
        im1=Image.open('icons/icon1.png')
        st.image(im1)
    with part2:
        st.markdown("**Deployed**")
    row1,row2,_=st.columns([0.25,2,3])
    with row1:
        im1=Image.open('icons/icon2.png')
        st.image(im1)
    with row2:
        st.markdown("**Grain is changed**")

def main():
    # st.markdown('<style>' + open('./style.css').read() + '</style>', unsafe_allow_html=True)
    with st.sidebar:
        tabs = on_hover_tabs(tabName=['Projects List', 'New Project', 'Update Project','Delete Project'], 
                            iconName=['economy', 'economy', 'economy','economy'], default_choice=0)
    if tabs =='Projects List':
        display()
    elif tabs == 'New Project':
        insert()
    elif tabs == 'Update Project':
        edit()
    elif tabs == 'Delete Project':
        delete()
    
if __name__=="__main__":
    main()
