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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.STOCK_AVAILABILITY_EBS_EXT(
	CLIENT VARCHAR(16777216) AS (CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))),
	MATERIAL_NUMBER VARCHAR(16777216) AS (CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))),
	PLANT VARCHAR(16777216) AS (CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))),
	MRP_ELEMENT VARCHAR(16777216) AS (CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))),
	MRP_ELEMENT_DATA VARCHAR(16777216) AS (CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))),
	RECEIPT_DT VARCHAR(16777216) AS (CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))),
	QUANTITY_REQUIRED VARCHAR(16777216) AS (CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))),
	ATP_QUANTITY VARCHAR(16777216) AS (CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))),
	RUN_DT VARCHAR(16777216) AS (CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))),
	CDC_FLAG VARCHAR(16777216) AS (CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))),
	CREATED_DTTM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c11') AS VARCHAR(16777216))),
	REGION_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c12') AS VARCHAR(16777216))),
	SOURCE_SYSTEM_NM VARCHAR(16777216) AS (CAST(GET(VALUE, 'c13') AS VARCHAR(16777216))),
	COUNTRY_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c14') AS VARCHAR(16777216))),
	SOURCE_SUBJECT_AREA VARCHAR(16777216) AS (CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))),
	COMPANY_CD VARCHAR(16777216) AS (CAST(GET(VALUE, 'c16') AS VARCHAR(16777216))),
	SOURCE_FILE_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c17') AS VARCHAR(16777216))),
	SOURCE_CREATED_DTTIME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c18') AS VARCHAR(16777216))),
	LZ_FILE_NAME VARCHAR(16777216) AS (CAST(GET(VALUE, 'c19') AS VARCHAR(16777216))),
	PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(GET(VALUE, 'c20') AS VARCHAR(16777216))),
	NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(GET(VALUE, 'c21') AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_QAT_MAP_EZ_ENT_GLBL_RO_STG/Inventory/SAP-EBS/Global/Delta/StockAvailability/Tsv/
auto_refresh=false
pattern='.*StockAvailability_.*[.]tsv'
file_format=EG_GSC_WHR_DEV_TSV_FF
;