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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.CS_ROOT_TRIGGER_EXT(
	ENTITY_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))),
	SOURCE_SYSTEM_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))),
	COUNTRY VARCHAR(16777216) AS (CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))),
	SUBJECT_AREA VARCHAR(16777216) AS (CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))),
	SUBSYSTEM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))),
	COMPANY_CODE VARCHAR(16777216) AS (CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))),
	PIPELINE_GROUP VARCHAR(16777216) AS (CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))),
	TRIGGER_GROUP VARCHAR(16777216) AS (CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))),
	CREATED_DTTM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))),
	LAST_UPDT_DTTM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/CostCenter/TriggerFile/Snowflake/
refresh_on_create=false
auto_refresh=false
pattern='.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.eg_dev_tsv_cn_cs_ff
;