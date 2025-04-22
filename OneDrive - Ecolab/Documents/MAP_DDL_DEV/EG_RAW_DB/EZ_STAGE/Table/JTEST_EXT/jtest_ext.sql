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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.JTEST_EXT
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_MAP_EZ_ENT_GLBL_RO_STG/Metadata/Snowflake/vTest/JSONs/cprefCountry_20211207215809_1.json/
refresh_on_create=false
auto_refresh=false
file_format=EG_DEV_JSON_RAW_TEST_FF
;