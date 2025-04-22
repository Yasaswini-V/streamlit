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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.BILLBACK_AGREEMENT_TEXT_EBS_EXT(
	AGREEMENT_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))),
	EXTERNAL_DESC VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))),
	AGREEMENT_DESC VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))),
	ODQ_CHANGE_MODE_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))),
	ODQ_ENTITY_CNTR_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c5')) AS VARCHAR(16777216))),
	SOURCE_FILE_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c6')) AS VARCHAR(16777216))),
	SOURCE_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c7')) AS VARCHAR(16777216))),
	CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c8')) AS VARCHAR(16777216))),
	PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c9')) AS VARCHAR(16777216))),
	REGION VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c10')) AS VARCHAR(16777216))),
	SOURCE_SYSTEM_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c11') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c11')) AS VARCHAR(16777216))),
	COUNTRY VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c12') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c12')) AS VARCHAR(16777216))),
	SUBJECT_AREA VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c13') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c13')) AS VARCHAR(16777216))),
	COMPANY_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c14') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c14')) AS VARCHAR(16777216))),
	FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c15')) AS VARCHAR(16777216))),
	FILE_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c16') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c16')) AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Agreement/SAP-EBS/Global/Delta/EccVistexBillbackAgreementDescription/Tsv/
refresh_on_create=false
auto_refresh=false
pattern='.*[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/EccVistexBillbackAgreementDescription.*[.]tsv'
file_format=ffa_ebs_tsv_ff
;