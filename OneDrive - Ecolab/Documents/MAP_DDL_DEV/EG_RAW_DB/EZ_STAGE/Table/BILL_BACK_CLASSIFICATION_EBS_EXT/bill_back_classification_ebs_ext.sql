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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.BILL_BACK_CLASSIFICATION_EBS_EXT(
	EBS_BILLBACK_CLASSIFICATION_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR)) = '', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))),
	EBS_BILLBACK_CLASSIFICATION_DESC VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR)) = '', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))),
	ODQ_CHANGE_MODE_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR)) = '', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))),
	ODQ_ENTITY_CNTR_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR)) = '', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))),
	AUD_SOURCE_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR)) = '', null, GET(VALUE, 'c5')) AS VARCHAR(16777216))),
	AUD_SOURCE_CREATED_DTTIME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR)) = '', null, GET(VALUE, 'c6')) AS VARCHAR(16777216))),
	AUD_CDC_FLAG VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR)) = '', null, GET(VALUE, 'c7')) AS VARCHAR(16777216))),
	AUD_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c8') AS VARCHAR)) = '', null, GET(VALUE, 'c8')) AS VARCHAR(16777216))),
	AUD_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c9') AS VARCHAR)) = '', null, GET(VALUE, 'c9')) AS VARCHAR(16777216))),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c10') AS VARCHAR)) = '', null, GET(VALUE, 'c10')) AS VARCHAR(16777216))),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c11') AS VARCHAR)) = '', null, GET(VALUE, 'c11')) AS VARCHAR(16777216))),
	AUD_REGION_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c12') AS VARCHAR)) = '', null, GET(VALUE, 'c12')) AS VARCHAR(16777216))),
	AUD_SOURCE_SYSTEM_NM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c13') AS VARCHAR)) = '', null, GET(VALUE, 'c13')) AS VARCHAR(16777216))),
	AUD_COUNTRY_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c14') AS VARCHAR)) = '', null, GET(VALUE, 'c14')) AS VARCHAR(16777216))),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c15') AS VARCHAR)) = '', null, GET(VALUE, 'c15')) AS VARCHAR(16777216))),
	AUD_COMPANY_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c16') AS VARCHAR)) = '', null, GET(VALUE, 'c16')) AS VARCHAR(16777216))),
	AUD_LZ_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c17') AS VARCHAR)) = '', null, GET(VALUE, 'c17')) AS VARCHAR(16777216))),
	AUD_LZ_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c18') AS VARCHAR)) = '', null, GET(VALUE, 'c18')) AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_MAP_EZ_ENT_GLBL_RO_STG/SalesMD/SAP-EBS/Global/Delta/EBSBillBackClassification/Tsv/
auto_refresh=false
pattern='.*[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/EBSBillBackClassification.*[.]tsv'
file_format=EZ_FILE_FORMAT
;