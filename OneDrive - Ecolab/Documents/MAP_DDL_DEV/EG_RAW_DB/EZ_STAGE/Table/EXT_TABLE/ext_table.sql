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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.EXT_TABLE(
	ID VARCHAR(16777216) AS (CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))),
	NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))),
	AGE VARCHAR(16777216) AS (CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_MAP_EZ_ENT_GLBL_RO_STG/test_1/testfile/escape.tsv/
refresh_on_create=false
auto_refresh=false
file_format=FFA_EBS_TSV_FF_TEMP
;