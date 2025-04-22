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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.RECORD_TYPE_SFDC_EXT(
	BUSINESS_PROCESS_ID VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))),
	CREATED_BY_ID VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))),
	CREATED_DATE TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c3')) AS TIMESTAMP_NTZ(9))),
	DESCRIPTION VARCHAR(1000) AS (CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))),
	DEVELOPER_NAME VARCHAR(300) AS (CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c5')) AS VARCHAR(16777216))),
	ID VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c6')) AS VARCHAR(16777216))),
	IS_ACTIVE VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c7')) AS VARCHAR(16777216))),
	LAST_MODIFIED_BY_ID VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c8')) AS VARCHAR(16777216))),
	LAST_MODIFIED_DATE TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c9')) AS TIMESTAMP_NTZ(9))),
	RECORD_TYPE_NM VARCHAR(300) AS (CAST(IFF((CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c10')) AS VARCHAR(16777216))),
	NAMESPACE_PREFIX VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c11') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c11')) AS VARCHAR(16777216))),
	SOBJECT_TYPE VARCHAR(300) AS (CAST(IFF((CAST(GET(VALUE, 'c12') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c12')) AS VARCHAR(16777216))),
	SYSTEM_MODSTAMP TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c13') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c13')) AS TIMESTAMP_NTZ(9))),
	CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c15')) AS TIMESTAMP_NTZ(9))),
	PRIMARY_KEY_MD5_VALUE VARCHAR(50) AS (CAST(IFF((CAST(GET(VALUE, 'c17') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c17')) AS VARCHAR(16777216))),
	REGION VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c19') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c19')) AS VARCHAR(16777216))),
	SOURCE_SYSTEM_NAME VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c20') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c20')) AS VARCHAR(16777216))),
	COUNTRY VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c21') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c21')) AS VARCHAR(16777216))),
	SUBJECT_AREA VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c22') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c22')) AS VARCHAR(16777216))),
	COMPANY_CD VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c23') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c23')) AS VARCHAR(16777216))),
	FILE_NAME VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c26') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c26')) AS VARCHAR(16777216))),
	FILE_CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(IFF((CAST(GET(VALUE, 'c27') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c27')) AS TIMESTAMP_NTZ(9))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Service/Salesforce/Global/CurrentDelta/RecordType/
auto_refresh=false
pattern='.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.EG_DEV_TSV_FF
;