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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.TRIGGER_INDICATOR_TBL (
	JOB_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 order,
	SOURCE_SYSTEM_NM VARCHAR(50),
	SOURCE_SUB_SYSTEM_NM VARCHAR(50),
	COUNTRY_CD VARCHAR(10),
	EXTRACTOR_NAME VARCHAR(256) NOT NULL,
	SOURCE_LAYER VARCHAR(10),
	TARGET_LAYER VARCHAR(10),
	DB_SCHEMA_NAME VARCHAR(100),
	SUBJECT_AREA VARCHAR(30),
	STATUS VARCHAR(100),
	LOG_TIME TIMESTAMP_NTZ(9),
	REGION VARCHAR(16777216),
	COMPANY_CD VARCHAR(16777216),
	primary key (JOB_ID)
);