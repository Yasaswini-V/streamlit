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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.CS_INDICATOR_ROOT_TRIGGER (
	ENTITY_NAME VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	SUBJECT_AREA VARCHAR(16777216),
	SUBSYSTEM VARCHAR(16777216),
	COMPANY_CODE VARCHAR(16777216),
	PIPELINE_GROUP VARCHAR(16777216),
	TRIGGER_GROUP VARCHAR(16777216),
	CREATED_DTTM VARCHAR(16777216),
	LAST_UPDT_DTTM VARCHAR(16777216)
);