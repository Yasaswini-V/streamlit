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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.CURR_HIST_ALIGN_EXT(
	COMPLETED_DATE DATE AS (CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR)) = '', null, GET(VALUE, 'c1')) AS DATE)),
	MONTH DATE AS (CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR)) = '', null, GET(VALUE, 'c2')) AS DATE)),
	ID VARCHAR(50) AS (CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR)) = '', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))),
	WORK_ORDER_NUMBER VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR)) = '', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))),
	ACCOUNT_ID VARCHAR(50) AS (CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR)) = '', null, GET(VALUE, 'c5')) AS VARCHAR(16777216))),
	ACCOUNT_NUMBER VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR)) = '', null, GET(VALUE, 'c6')) AS VARCHAR(16777216))),
	ALIGN VARCHAR(30) AS (CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR)) = '', null, GET(VALUE, 'c7')) AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EXT_STG_SFDC/Service/Salesforce/Global/Delta/ManualAlignmentStamping/
auto_refresh=false
file_format=MANUAL_ALIGN_STAMP_CSV
;