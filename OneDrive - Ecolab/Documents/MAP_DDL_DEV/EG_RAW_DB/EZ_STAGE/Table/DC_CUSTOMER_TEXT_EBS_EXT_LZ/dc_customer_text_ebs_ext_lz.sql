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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.DC_CUSTOMER_TEXT_EBS_EXT_LZ(
	DI_SEQUENCE_NUM NUMBER(10,0) AS (TO_NUMBER(IFF((CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c1'))), 10, 0)),
	DI_OPERATION_TYPE_CD VARCHAR(1) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c2'))) AS VARCHAR(1))),
	CUSTOMER_NUM VARCHAR(10) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c3'))) AS VARCHAR(10))),
	NAME_DESC VARCHAR(35) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))) = '', null, IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = 'null', null, GET(VALUE, 'c4'))) AS VARCHAR(35))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_QAT_MAP_EZ_ENT_GLBL_RO_STG/Metadata/Test/0CUSTOMER_TEXT_20240214051443.tsv/
refresh_on_create=false
auto_refresh=false
pattern='.*/.*/.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.eg_dev_tsv_cn_ff
;