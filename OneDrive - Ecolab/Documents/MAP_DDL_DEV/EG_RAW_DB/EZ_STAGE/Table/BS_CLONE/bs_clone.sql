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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.BS_CLONE (
	LANGUAGE_KEY_CD VARCHAR(16777216),
	SEGMENT_CD VARCHAR(16777216),
	TEXT_FIELD_NAME VARCHAR(16777216),
	ODQ_CHANGE_MODE VARCHAR(16777216),
	ODQ_ENTITY_CNTR VARCHAR(16777216),
	CREATED_DTTM VARCHAR(16777216),
	UPDATED_DTTM VARCHAR(16777216),
	JOB_ID VARCHAR(16777216),
	IS_DELETED_IND VARCHAR(16777216),
	AUD_CDC_FLAG VARCHAR(16777216),
	AUD_CREATED_DTTM VARCHAR(16777216),
	AUD_FILE_NAME VARCHAR(16777216),
	AUD_REGION_CD VARCHAR(16777216),
	AUD_SOURCE_SYSTEM_NM VARCHAR(16777216),
	AUD_COUNTRY_CD VARCHAR(16777216),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(16777216),
	AUD_COMPANY_CD VARCHAR(16777216),
	AUD_SOURCE_FILE_NAME VARCHAR(16777216),
	AUD_SOURCE_CREATED_DTTIME VARCHAR(16777216),
	AUD_LZ_FILE_NAME VARCHAR(16777216),
	AUD_LZ_CREATED_DTTM VARCHAR(16777216),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216),
	AUD_IL_CREATED_DTTM TIMESTAMP_NTZ(9),
	AUD_IL_UPDATED_DTTM TIMESTAMP_NTZ(9)
);