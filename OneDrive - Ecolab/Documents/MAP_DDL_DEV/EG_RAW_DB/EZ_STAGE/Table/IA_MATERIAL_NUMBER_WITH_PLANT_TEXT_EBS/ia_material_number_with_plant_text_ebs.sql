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
  create or alter TRANSIENT TABLE {EG_RAW_DB}.EZ_STAGE.IA_MATERIAL_NUMBER_WITH_PLANT_TEXT_EBS (
	PLANT_CD VARCHAR(4),
	PRODUCT_NUM VARCHAR(20),
	LANGUAGE_CD VARCHAR(1),
	MAT_PLANT_MED_DESC VARCHAR(60000),
	ODQ_CHANGE_MODE_CD VARCHAR(60000),
	ODQ_ENTITY_CNTR_CD NUMBER(19,3),
	AUD_SOURCE_FILE_NAME VARCHAR(100),
	AUD_SOURCE_CREATED_DTTIME TIMESTAMP_NTZ(9),
	AUD_CDC_FLAG VARCHAR(5),
	AUD_CREATED_DTTM TIMESTAMP_NTZ(9),
	AUD_FILE_NAME VARCHAR(100),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(1000),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(1000),
	AUD_REGION_CD VARCHAR(10),
	AUD_SOURCE_SYSTEM_NM VARCHAR(20),
	AUD_COUNTRY_CD VARCHAR(10),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(50),
	AUD_COMPANY_CD VARCHAR(10),
	AUD_LZ_FILE_NAME VARCHAR(100),
	AUD_LZ_CREATED_DTTM TIMESTAMP_NTZ(9),
	AUD_IL_CREATED_DTTM TIMESTAMP_NTZ(9),
	AUD_IL_UPDATED_DTTM TIMESTAMP_NTZ(9)
);