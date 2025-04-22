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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.BUSINESS_UNIT_MDM(
	ROWID_OBJECT VARCHAR(16777216) AS (CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))),
	CREATOR VARCHAR(16777216) AS (CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))),
	CREATE_DATE TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c3') AS TIMESTAMP_NTZ(9))),
	UPDATED_BY VARCHAR(16777216) AS (CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))),
	LAST_UPDATE_DATE TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c5') AS TIMESTAMP_NTZ(9))),
	CONSOLIDATION_IND NUMBER(38,0) AS (TO_NUMBER(GET(VALUE, 'c6'))),
	DELETED_IND NUMBER(38,0) AS (TO_NUMBER(GET(VALUE, 'c7'))),
	DELETED_BY VARCHAR(16777216) AS (CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))),
	DELETED_DATE TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c9') AS TIMESTAMP_NTZ(9))),
	LAST_ROWID_SYSTEM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))),
	DIRTY_IND NUMBER(38,0) AS (TO_NUMBER(GET(VALUE, 'c11'))),
	INTERACTION_ID NUMBER(38,0) AS (TO_NUMBER(GET(VALUE, 'c12'))),
	HUB_STATE_IND NUMBER(38,0) AS (TO_NUMBER(GET(VALUE, 'c13'))),
	CM_DIRTY_IND NUMBER(38,0) AS (TO_NUMBER(GET(VALUE, 'c14'))),
	BUS_UNIT_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))),
	BUS_UNIT_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c16') AS VARCHAR(16777216))),
	START_DATE TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c17') AS TIMESTAMP_NTZ(9))),
	DISPLAY_DESCRIPTION VARCHAR(16777216) AS (CAST(GET(VALUE, 'c18') AS VARCHAR(16777216))),
	END_DATE TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c19') AS TIMESTAMP_NTZ(9))),
	EBS_BUS_UNIT_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c20') AS VARCHAR(16777216))),
	EBS_DISPLAY_DESCRIPTION VARCHAR(16777216) AS (CAST(GET(VALUE, 'c21') AS VARCHAR(16777216))),
	CREATED_DTTM TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c22') AS TIMESTAMP_NTZ(9))),
	LAST_UPDT_DTTM TIMESTAMP_NTZ(9) AS (CAST(GET(VALUE, 'c23') AS TIMESTAMP_NTZ(9))),
	JOB_ID VARCHAR(16777216) AS (CAST(GET(VALUE, 'c24') AS VARCHAR(16777216))),
	FILE_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c25') AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_MAP_LEA_RO_STG/Exploration/EnterpriseBackbone/MDM/AZURE_BU/
refresh_on_create=false
auto_refresh=false
pattern='.*/.*/.*[.]tsv'
file_format=EG_DEV_TSV_WH_FF
;