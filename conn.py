from snowflake.snowpark import Session
import streamlit as st

@st.cache_resource(ttl=6000, experimental_allow_widgets=True) 
def connection():
    try: 
        connection_parameters={
        'account':'ecolab.east-us-2.azure',
        'user':'YASASWINI.V@ECOLAB.COM',
        'warehouse':'EG_DEV_SVC_ETL_ENGR_WH',
        'role':'EG_DEV_ENGR_FR',
        'database':'EG_DEV_WRKS_ENGR_DB',
        'schema':'SHARED_COMMON',
        'authenticator':'externalbrowser',
        'client_session_keep_alive' : 'true'
        }

        session = Session.builder.configs(connection_parameters).create()
        return session
    except Exception as e:
        try:
            st.write(e.message)
            st.write(e.error_code)
            st.write(e.sfqid)
            st.write(e.query)    
        except:
            st.write(e)
        st.error('It Failed !!')
        return False
 