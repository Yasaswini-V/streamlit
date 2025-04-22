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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.CUSTOMER_HIERARCHY_MDM(
	GBACUSH05 VARCHAR(16777216) AS (CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))),
	GBACH05_T VARCHAR(16777216) AS (CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))),
	GBACUSH04 VARCHAR(16777216) AS (CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))),
	GBACH04_T VARCHAR(16777216) AS (CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))),
	GBACUSH03 VARCHAR(16777216) AS (CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))),
	GBACH03_T VARCHAR(16777216) AS (CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))),
	GBACUSH02 VARCHAR(16777216) AS (CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))),
	GBACH02_T VARCHAR(16777216) AS (CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))),
	GBACUSH01 VARCHAR(16777216) AS (CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))),
	GBACH01_T VARCHAR(16777216) AS (CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))),
	LST_BC_HIER_DM_UPDT_DT TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c11') AS TIMESTAMP_NTZ(9))),
	CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c12') AS TIMESTAMP_NTZ(9))),
	LAST_UPDT_DTTM TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c13') AS TIMESTAMP_NTZ(9))),
	JOB_ID VARCHAR(16777216) AS (CAST(GET(VALUE, 'c14') AS VARCHAR(16777216))),
	FILE_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_MAP_LEA_RO_STG/Exploration/EnterpriseBackbone/MDM/AZURE_BASE_CUSTOMER_HIERARCHY/
refresh_on_create=false
auto_refresh=false
pattern='.*/.*/.*[.]tsv'
file_format=eg_prd_mdmap_tsv_ff
;