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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.HOLIDAY_WORKDAY_EXT(
	CALENDAR VARCHAR(1000) AS (CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))),
	COUNTRY_CODE VARCHAR(100) AS (CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))),
	CALENDAR_EVENT VARCHAR(500) AS (CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))),
	CALENDAR_DATE DATE AS (CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c4')) AS DATE)))
location=@{EG_RAW_DB}.EZ_STAGE.EG_QAT_MAP_EZ_ENT_GLBL_RO_STG/Hr/WorkDay/Global/CurrentDelta/Holiday/
auto_refresh=false
pattern='.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.TSV_FILE_FMT_SFDC
;