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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.ALTERYX_USERS_TABLE (
	ID NUMBER(38,0) autoincrement start 1 increment 1 noorder,
	NAME VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	NUMBER NUMBER(38,0),
	ROLE VARCHAR(16777216)
);