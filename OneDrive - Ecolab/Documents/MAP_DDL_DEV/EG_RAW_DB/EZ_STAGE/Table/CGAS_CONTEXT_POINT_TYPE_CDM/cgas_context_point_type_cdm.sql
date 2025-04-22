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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.CGAS_CONTEXT_POINT_TYPE_CDM (
	CONTEXT_POINT_TYPE_CODE VARCHAR(16777216),
	PARENT_CONTEXT_POINT_TYPE_CODE VARCHAR(16777216),
	CONTEXT_POINT_TYPE_DESCRIPTION VARCHAR(16777216),
	CREATED_BY VARCHAR(16777216),
	CREATED_ON TIMESTAMP_NTZ(9),
	MODIFIED_BY VARCHAR(16777216),
	MODIFIED_ON TIMESTAMP_NTZ(9),
	IS_ACTIVE VARCHAR(16777216),
	DATA_INTG_EXEC_ID VARCHAR(16777216),
	AUD_SOURCE_FILE_NAME VARCHAR(16777216),
	AUD_SOURCE_CREATED_DTTIME TIMESTAMP_NTZ(9),
	AUD_CDC_FLAG VARCHAR(16777216),
	AUD_CREATED_DTTM TIMESTAMP_NTZ(9),
	AUD_FILE_NAME VARCHAR(16777216),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216),
	AUD_REGION_CD VARCHAR(16777216),
	AUD_SOURCE_SYSTEM_NM VARCHAR(16777216),
	AUD_COUNTRY_CD VARCHAR(16777216),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(16777216),
	AUD_COMPANY_CD VARCHAR(16777216),
	AUD_LZ_FILE_NAME VARCHAR(16777216),
	AUD_LZ_CREATED_DTTM TIMESTAMP_NTZ(9),
	CREATED_DT_TM_IL TIMESTAMP_NTZ(9),
	LAST_UPDT_TM_IL TIMESTAMP_NTZ(9),
	constraint PK primary key (CONTEXT_POINT_TYPE_CODE)
);