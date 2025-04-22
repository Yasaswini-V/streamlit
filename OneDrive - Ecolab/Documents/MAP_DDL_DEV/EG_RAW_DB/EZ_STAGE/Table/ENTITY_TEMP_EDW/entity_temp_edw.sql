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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.ENTITY_TEMP_EDW (
	ROWNUM NUMBER(18,0),
	ENTITY_KEY NUMBER(38,0),
	ENTITY_NM VARCHAR(16777216),
	FILE_FORMAT VARCHAR(16777216),
	SOURCE_FILE_PATH VARCHAR(16777216)
);