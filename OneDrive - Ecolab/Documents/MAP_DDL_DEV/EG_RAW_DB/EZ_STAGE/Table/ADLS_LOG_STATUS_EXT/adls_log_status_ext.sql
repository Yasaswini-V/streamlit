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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.ADLS_LOG_STATUS_EXT(
	SOURCE_SYSTEM_NM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))),
	SOURCE_SUB_SYSTEM_NM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))),
	COUNTRY_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))),
	EXTRACTOR_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))),
	SOURCE_LAYER VARCHAR(16777216) AS (CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))),
	TARGET_LAYER VARCHAR(16777216) AS (CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))),
	DB_SCHEMA_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))),
	SUBJECT_AREA VARCHAR(16777216) AS (CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))),
	STATUS VARCHAR(16777216) AS (CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))),
	LOG_TIME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c11') AS VARCHAR(16777216))),
	REGION VARCHAR(16777216) AS (CAST(GET(VALUE, 'c12') AS VARCHAR(16777216))),
	COMPANY_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c13') AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Log/
refresh_on_create=false
auto_refresh=false
pattern='.*[.]tsv'
file_format=eg_dev_tsv_ff
;