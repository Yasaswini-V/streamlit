import snowflake.connector
from PIL import Image
import io
import base64
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from datetime import date
import streamlit as st
import pages.Update as Update
import pages.Insert as insert
import pages.Delete as delete
import conn


con=conn.connect_local()

def get_df(opt):
    if opt=='table_details':
        df =pd.DataFrame(con.cursor().execute(f" SELECT * FROM EG_DEV_WRKS_ENGR_DB.SHARED_COMMON.TABLE_DETAILS_DESC WHERE DEACTIVATE='FALSE'").fetch_pandas_all())
        return df

def color_coding(row):
    if row.DEPLOYMENT_DATE < date.today() and row.GRAIN_CHANGED=='YES':
        return ['background-color:#93AFED'] * len(row)
    elif row.DEPLOYMENT_DATE < date.today() and row.GRAIN_CHANGED=='NO':
        return ['background-color:#B9CAF0 '] * len(row) 
    elif row.GRAIN_CHANGED == 'YES':
        return ['background-color:#e2eafc'] * len(row) 
    else:
        return [''] * len(row)

def main():
    im=Image.open('icons/Table.png')
    img_bytes = io.BytesIO()
    im.save(img_bytes, format="PNG")
    img_bytes = img_bytes.getvalue()

    # Encode bytes as base64
    img_str = base64.b64encode(img_bytes).decode()

    # Define HTML code with image and text
    html_code = f"""
            <div style="display: flex; align-items: center;">
                <p style="margin: 0;font-size:40px;font-weight:bold;font-family: 'Agbalumo', serif"> Display  </p>
                <img src="data:image/png;base64,{img_str}" alt="Icon" style="width: 40px; height: 40px; margin-left: 15px;">
            </div>"""

    st.markdown(html_code, unsafe_allow_html=True)
    st.write("")
    df=get_df('table_details')
    styled_df=df.style.apply(color_coding,axis=1)
    st.dataframe(styled_df,hide_index=True,column_config={"ID": None,"DEACTIVATE":None})
    st.markdown("""
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; border-radius: 50%;background-color:#e2eafc; border: 2px solid black; margin-right: 10px;"></div>
            <p style="margin: 0;">Grain is changed</p>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; border-radius: 50%;background-color:#B9CAF0; border: 2px solid black; margin-right: 10px;"></div>
            <p style="margin: 0;">Deployment date passed, Grain is not changed</p>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; border-radius: 50%;background-color:#93AFED; border: 2px solid black; margin-right: 10px;"></div>
            <p style="margin: 0;">Deployment date passed, Grain is changed.</p>
        </div>
        """, unsafe_allow_html=True)
        
if __name__=="__main__":
    main()
