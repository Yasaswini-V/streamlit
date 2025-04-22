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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.DAS_PERMISSION_CDM_EXT(
	PERMISSION_CODE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR)) = '', null, GET(VALUE, 'c1')) AS VARCHAR(16777216))),
	PERMISSION_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR)) = '', null, GET(VALUE, 'c2')) AS VARCHAR(16777216))),
	IS_ACTIVE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR)) = '', null, GET(VALUE, 'c3')) AS VARCHAR(16777216))),
	APPLICATION_CODE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR)) = '', null, GET(VALUE, 'c4')) AS VARCHAR(16777216))),
	CREATED_BY VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR)) = '', null, GET(VALUE, 'c5')) AS VARCHAR(16777216))),
	CREATED_ON VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR)) = '', null, GET(VALUE, 'c6')) AS VARCHAR(16777216))),
	MODIFIED_BY VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR)) = '', null, GET(VALUE, 'c7')) AS VARCHAR(16777216))),
	MODIFIED_ON VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c8') AS VARCHAR)) = '', null, GET(VALUE, 'c8')) AS VARCHAR(16777216))),
	DATA_INTG_EXEC_ID VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c9') AS VARCHAR)) = '', null, GET(VALUE, 'c9')) AS VARCHAR(16777216))),
	AUD_SOURCE_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c10') AS VARCHAR)) = '', null, GET(VALUE, 'c10')) AS VARCHAR(16777216))),
	AUD_SOURCE_CREATED_DTTIME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c11') AS VARCHAR)) = '', null, GET(VALUE, 'c11')) AS VARCHAR(16777216))),
	AUD_CDC_FLAG VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c12') AS VARCHAR)) = '', null, GET(VALUE, 'c12')) AS VARCHAR(16777216))),
	AUD_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c13') AS VARCHAR)) = '', null, GET(VALUE, 'c13')) AS VARCHAR(16777216))),
	AUD_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c14') AS VARCHAR)) = '', null, GET(VALUE, 'c14')) AS VARCHAR(16777216))),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c15') AS VARCHAR)) = '', null, GET(VALUE, 'c15')) AS VARCHAR(16777216))),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c16') AS VARCHAR)) = '', null, GET(VALUE, 'c16')) AS VARCHAR(16777216))),
	AUD_REGION_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c17') AS VARCHAR)) = '', null, GET(VALUE, 'c17')) AS VARCHAR(16777216))),
	AUD_SOURCE_SYSTEM_NM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c18') AS VARCHAR)) = '', null, GET(VALUE, 'c18')) AS VARCHAR(16777216))),
	AUD_COUNTRY_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c19') AS VARCHAR)) = '', null, GET(VALUE, 'c19')) AS VARCHAR(16777216))),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c20') AS VARCHAR)) = '', null, GET(VALUE, 'c20')) AS VARCHAR(16777216))),
	AUD_COMPANY_CD VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c21') AS VARCHAR)) = '', null, GET(VALUE, 'c21')) AS VARCHAR(16777216))),
	AUD_LZ_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c22') AS VARCHAR)) = '', null, GET(VALUE, 'c22')) AS VARCHAR(16777216))),
	AUD_LZ_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(GET(VALUE, 'c23') AS VARCHAR)) = '', null, GET(VALUE, 'c23')) AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_MAP_EZ_ENT_GLBL_RO_STG/Permission/CDM/Global/Delta/DenormalizedAccessSync_Permission/
refresh_on_create=false
auto_refresh=false
pattern='.*[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/DenormalizedAccessSync_Permission.*[.]tsv'
file_format=CDS_INST_3D_TSV_FF
;