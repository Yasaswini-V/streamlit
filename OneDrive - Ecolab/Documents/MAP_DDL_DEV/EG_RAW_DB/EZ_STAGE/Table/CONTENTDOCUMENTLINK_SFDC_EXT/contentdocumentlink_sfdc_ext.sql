-------------------------------------------------------------------------------
  -- Author       : Azure Devops Bot
  -- Project      : 
  -- Database     : EG_RAW_DB
  -- Schema       : EZ_STAGE
  -- Object Type  : Table
  -------------------------------------------------------------------------------
  -- Change Log 
  -------------------------------------------------------------------------------
  -- Version |Modified Date |Engineer Name                 |Change description
  -------------------------------------------------------------------------------
  -- v.1.0   |2025-04-22    |Azure Devops Bot               |Initial DDL extracted from snowflake 
  --
  -------------------------------------------------------------------------------
  create or alter external table {EG_RAW_DB}.EZ_STAGE.CONTENTDOCUMENTLINK_SFDC_EXT(
	CONTENT_DOCUMENT_ID VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))),
	ID VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))),
	IS_DELETED VARCHAR(10) AS (CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))),
	LINKED_ENTITY_ID VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))),
	SHARE_TYPE VARCHAR(50) AS (CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c5')) AS VARCHAR(16777216))),
	SYSTEM_MODSTAMP TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c6')) AS TIMESTAMP_NTZ(9))),
	VISIBILITY VARCHAR(50) AS (CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c7')) AS VARCHAR(16777216))),
	AUD_CDC_FLAG VARCHAR(10) AS (CAST(IFF((CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c8')) AS VARCHAR(16777216))),
	AUD_CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c9')) AS TIMESTAMP_NTZ(9))),
	AUD_FILE_NAME VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c10')) AS VARCHAR(16777216))),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(50) AS (CAST(IFF((CAST(GET(VALUE, 'c11') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c11')) AS VARCHAR(16777216))),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(50) AS (CAST(IFF((CAST(GET(VALUE, 'c12') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c12')) AS VARCHAR(16777216))),
	AUD_REGION_CD VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c13') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c13')) AS VARCHAR(16777216))),
	AUD_SOURCE_SYSTEM_NM VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c14') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c14')) AS VARCHAR(16777216))),
	AUD_COUNTRY_CD VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c15')) AS VARCHAR(16777216))),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c16') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c16')) AS VARCHAR(16777216))),
	AUD_COMPANY_CD VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c17') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c17')) AS VARCHAR(16777216))),
	AUD_SOURCE_FILE_NAME VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c18') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c18')) AS VARCHAR(16777216))),
	AUD_SOURCE_CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c19') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c19')) AS TIMESTAMP_NTZ(9))),
	AUD_LZ_FILE_NAME VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c20') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c20')) AS VARCHAR(16777216))),
	AUD_LZ_CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c21') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c21')) AS TIMESTAMP_NTZ(9))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Service/Salesforce/Global/CurrentDelta/ContentDocumentLink/
auto_refresh=false
pattern='.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.EG_DEV_TSV_FF
;