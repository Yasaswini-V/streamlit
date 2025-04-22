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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.GPS_MISSING_FILE_EXT_TEST(
	ID VARIANT AS (IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c1'))),
	TIMESTAMP VARIANT AS (IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c2'))),
	HARDWAREID VARIANT AS (IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c3'))),
	MOBILENAME VARIANT AS (IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c4'))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Service/Salesforce/Global/GPS_MISSING_DATA/
auto_refresh=false
pattern='.*[.]csv'
file_format=MANUAL_ALIGN_STAMP_CSV
;