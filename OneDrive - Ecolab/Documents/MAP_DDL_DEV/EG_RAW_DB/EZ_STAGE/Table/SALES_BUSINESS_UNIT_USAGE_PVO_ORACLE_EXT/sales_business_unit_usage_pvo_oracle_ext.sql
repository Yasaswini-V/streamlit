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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.SALES_BUSINESS_UNIT_USAGE_PVO_ORACLE_EXT(
	BUSINESS_UNIT_BUSINESS_UNIT_ID VARCHAR(16777216) AS (CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))),
	BUSINESS_FUNCTION_BUSINESS_FUNCTION_CODE VARCHAR(16777216) AS (CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))),
	BUSINESS_UNIT_ID VARCHAR(16777216) AS (CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))),
	AUD_SOURCE_FILE_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))),
	AUD_SOURCE_CREATED_DTTIME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))),
	AUD_CDC_FLAG VARCHAR(16777216) AS (CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))),
	AUD_CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c7') AS TIMESTAMP_NTZ(9))),
	AUD_FILE_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))),
	AUD_REGION_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c11') AS VARCHAR(16777216))),
	AUD_SOURCE_SYSTEM_NM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c12') AS VARCHAR(16777216))),
	AUD_COUNTRY_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c13') AS VARCHAR(16777216))),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(16777216) AS (CAST(GET(VALUE, 'c14') AS VARCHAR(16777216))),
	AUD_COMPANY_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))),
	AUD_LZ_FILE_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c16') AS VARCHAR(16777216))),
	AUD_LZ_CREATED_DTTM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c17') AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Sales/ECL_ORA/Global/Delta/BusinessUnitUsagePVOSales/Tsv/
refresh_on_create=false
auto_refresh=false
pattern='.*BusinessUnitUsagePVOSales_.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.EG_DEV_TSV_FF
;