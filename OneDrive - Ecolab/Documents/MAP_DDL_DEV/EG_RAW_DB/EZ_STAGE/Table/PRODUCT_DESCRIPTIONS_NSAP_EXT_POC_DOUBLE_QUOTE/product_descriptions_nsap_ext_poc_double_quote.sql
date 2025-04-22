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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.PRODUCT_DESCRIPTIONS_NSAP_EXT_POC_DOUBLE_QUOTE(
	CLIENT VARCHAR(10) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c1'))) AS VARCHAR(10))),
	PRODUCT_NUM VARCHAR(250) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c2'))) AS VARCHAR(250))),
	LANGUAGE_KEY VARCHAR(250) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c3'))) AS VARCHAR(250))),
	PRODUCT_DESCRIPTION VARCHAR(250) AS (REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c4'))) AS VARCHAR(250)), '[±]', ''), '(^"")|(""$)', '"'), '^"|"$')),
	PRODUCT_DESCRIPTION_UPPER_CASE VARCHAR(250) AS (REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c5')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c5'))) AS VARCHAR(250)), '[±]', ''), '""', '"'), '^"|"$')))
location=@{EG_RAW_DB}.EZ_STAGE.EG_QAT_MAP_EZ_ENT_GLBL_RO_STG/Product/SAP-NSAP/Global/Delta/ProductDescriptions/Tsv/2024/07/15/
refresh_on_create=false
auto_refresh=false
pattern='.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.EG_DEV_TSV_FF_POC_DOUBLE_QUOTE
;