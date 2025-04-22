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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.PEST_CUSTOMER_CAN_US_CA_PEST_EXT_TEST(
	NAME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c1'))) AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_MAP_EZ_ENT_GLBL_RO_STG/Customer/US_CA_Pest/CA/CurrentDelta/PEST_CUSTOMER_CAN1/PEST_CUSTOMER_CAN_20240813151931.tsv/
refresh_on_create=false
auto_refresh=false
file_format={EG_RAW_DB}.EZ_STAGE.EG_DEV_TSV_FF
;