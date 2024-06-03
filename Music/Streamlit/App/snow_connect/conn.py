import streamlit as st
import snowflake.connector
import pandas as pd
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import configparser

@st.cache_resource(ttl=8600,show_spinner=False)
def connect():
  con=connect_cloud()
  
  return con

def connect_cloud():
  KVUri = f"https://secret-svc-mapplan-dev.vault.azure.net/"
  credential = DefaultAzureCredential()
  client = SecretClient(vault_url=KVUri, credential=credential)
  retrieved_secret=client.get_secret("privateKey")
  
  prk= prk= retrieved_secret.value.replace('\\n','\n')
  #st.text(prk)
  
  f = open( 'mapplanner_rsa_key.p8', 'w' )
  f.write( prk )
  f.close()

  passphrase='Ec0lab@!234'
  with open("mapplanner_rsa_key.p8", "rb") as key:
      p_key= serialization.load_pem_private_key(
          key.read(),
          password=passphrase.encode(),
          backend=default_backend()
      )
 
  pkb = p_key.private_bytes(
      encoding=serialization.Encoding.DER,
      format=serialization.PrivateFormat.PKCS8,
      encryption_algorithm=serialization.NoEncryption())

  conn = snowflake.connector.connect(
    account='ecolab.east-us-2.azure',
    user='4c11d22b-cad5-4352-902f-2eae8138ab34',
    private_key=pkb,
    warehouse='EG_MAP_PLANNER_DEV_SVC_ETL_WH',
    role='EG_MAP_PLANNER_DEV_ENGR_FR',
    database='EG_DEV_LOG_DB',
    schema='MAP_PLANNER',
    client_session_keep_alive = 'true'
    )
  return conn;

def connect_local():
  #st.text ('Local connect')
  conn = snowflake.connector.connect(
    account='ecolab.east-us-2.azure',
    user='4c11d22b-cad5-4352-902f-2eae8138ab34',
    warehouse='EG_MAP_PLANNER_DEV_SVC_ETL_WH',
    role='EG_MAP_PLANNER_DEV_OPS_FR',
    database='EG_DEV_LOG_DB',
    schema='MAP_PLANNER',
    authenticator='externalbrowser',
    client_session_keep_alive = 'true'
    )
  return conn;
