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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.FB_FSS_COUPONS_CNZTV_EBS_EXT(
	CLIENT_CD VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR)) = 'null', null, GET(VALUE, 'c1')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c1') AS VARCHAR)) = 'null', null, GET(VALUE, 'c1'))) AS VARCHAR(16777216))),
	CONDITION_RECORD_ID VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR)) = 'null', null, GET(VALUE, 'c2')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c2') AS VARCHAR)) = 'null', null, GET(VALUE, 'c2'))) AS VARCHAR(16777216))),
	SALES_ORGANIZATION_ID VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR)) = 'null', null, GET(VALUE, 'c3')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c3') AS VARCHAR)) = 'null', null, GET(VALUE, 'c3'))) AS VARCHAR(16777216))),
	ECC_SALES_ORG_ID VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR)) = 'null', null, GET(VALUE, 'c4')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c4') AS VARCHAR)) = 'null', null, GET(VALUE, 'c4'))) AS VARCHAR(16777216))),
	DISTRIBUTION_CHANNEL_CD VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR)) = 'null', null, GET(VALUE, 'c5')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c5') AS VARCHAR)) = 'null', null, GET(VALUE, 'c5'))) AS VARCHAR(16777216))),
	DIVISION_CD VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR)) = 'null', null, GET(VALUE, 'c6')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c6') AS VARCHAR)) = 'null', null, GET(VALUE, 'c6'))) AS VARCHAR(16777216))),
	EXTERNAL_ID VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR)) = 'null', null, GET(VALUE, 'c7')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c7') AS VARCHAR)) = 'null', null, GET(VALUE, 'c7'))) AS VARCHAR(16777216))),
	TEXT1 VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c8') AS VARCHAR)) = 'null', null, GET(VALUE, 'c8')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c8') AS VARCHAR)) = 'null', null, GET(VALUE, 'c8'))) AS VARCHAR(16777216))),
	RATE_MIN_ORDER VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c9') AS VARCHAR)) = 'null', null, GET(VALUE, 'c9')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c9') AS VARCHAR)) = 'null', null, GET(VALUE, 'c9'))) AS VARCHAR(16777216))),
	RATE_CURR_UNIT VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c10') AS VARCHAR)) = 'null', null, GET(VALUE, 'c10')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c10') AS VARCHAR)) = 'null', null, GET(VALUE, 'c10'))) AS VARCHAR(16777216))),
	FSS_FREE_SHP VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c11') AS VARCHAR)) = 'null', null, GET(VALUE, 'c11')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c11') AS VARCHAR)) = 'null', null, GET(VALUE, 'c11'))) AS VARCHAR(16777216))),
	DEL_FLAG VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c12') AS VARCHAR)) = 'null', null, GET(VALUE, 'c12')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c12') AS VARCHAR)) = 'null', null, GET(VALUE, 'c12'))) AS VARCHAR(16777216))),
	CREATED_BY_USER_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c13') AS VARCHAR)) = 'null', null, GET(VALUE, 'c13')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c13') AS VARCHAR)) = 'null', null, GET(VALUE, 'c13'))) AS VARCHAR(16777216))),
	CONDITION_RECORD_CREATED_DT VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c14') AS VARCHAR)) = 'null', null, GET(VALUE, 'c14')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c14') AS VARCHAR)) = 'null', null, GET(VALUE, 'c14'))) AS VARCHAR(16777216))),
	CREATED_TIME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c15') AS VARCHAR)) = 'null', null, GET(VALUE, 'c15')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c15') AS VARCHAR)) = 'null', null, GET(VALUE, 'c15'))) AS VARCHAR(16777216))),
	CHANGED_BY_USER_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c16') AS VARCHAR)) = 'null', null, GET(VALUE, 'c16')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c16') AS VARCHAR)) = 'null', null, GET(VALUE, 'c16'))) AS VARCHAR(16777216))),
	LAST_CHANGED_DT VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c17') AS VARCHAR)) = 'null', null, GET(VALUE, 'c17')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c17') AS VARCHAR)) = 'null', null, GET(VALUE, 'c17'))) AS VARCHAR(16777216))),
	CHANGED_TIME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c18') AS VARCHAR)) = 'null', null, GET(VALUE, 'c18')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c18') AS VARCHAR)) = 'null', null, GET(VALUE, 'c18'))) AS VARCHAR(16777216))),
	AUD_SOURCE_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c19') AS VARCHAR)) = 'null', null, GET(VALUE, 'c19')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c19') AS VARCHAR)) = 'null', null, GET(VALUE, 'c19'))) AS VARCHAR(16777216))),
	AUD_SOURCE_CREATED_DTTIME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c20') AS VARCHAR)) = 'null', null, GET(VALUE, 'c20')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c20') AS VARCHAR)) = 'null', null, GET(VALUE, 'c20'))) AS VARCHAR(16777216))),
	AUD_CDC_FLAG VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c21') AS VARCHAR)) = 'null', null, GET(VALUE, 'c21')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c21') AS VARCHAR)) = 'null', null, GET(VALUE, 'c21'))) AS VARCHAR(16777216))),
	AUD_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c22') AS VARCHAR)) = 'null', null, GET(VALUE, 'c22')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c22') AS VARCHAR)) = 'null', null, GET(VALUE, 'c22'))) AS VARCHAR(16777216))),
	AUD_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c23') AS VARCHAR)) = 'null', null, GET(VALUE, 'c23')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c23') AS VARCHAR)) = 'null', null, GET(VALUE, 'c23'))) AS VARCHAR(16777216))),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c24') AS VARCHAR)) = 'null', null, GET(VALUE, 'c24')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c24') AS VARCHAR)) = 'null', null, GET(VALUE, 'c24'))) AS VARCHAR(16777216))),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c25') AS VARCHAR)) = 'null', null, GET(VALUE, 'c25')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c25') AS VARCHAR)) = 'null', null, GET(VALUE, 'c25'))) AS VARCHAR(16777216))),
	AUD_REGION_CD VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c26') AS VARCHAR)) = 'null', null, GET(VALUE, 'c26')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c26') AS VARCHAR)) = 'null', null, GET(VALUE, 'c26'))) AS VARCHAR(16777216))),
	AUD_SOURCE_SYSTEM_NM VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c27') AS VARCHAR)) = 'null', null, GET(VALUE, 'c27')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c27') AS VARCHAR)) = 'null', null, GET(VALUE, 'c27'))) AS VARCHAR(16777216))),
	AUD_COUNTRY_CD VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c28') AS VARCHAR)) = 'null', null, GET(VALUE, 'c28')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c28') AS VARCHAR)) = 'null', null, GET(VALUE, 'c28'))) AS VARCHAR(16777216))),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c29') AS VARCHAR)) = 'null', null, GET(VALUE, 'c29')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c29') AS VARCHAR)) = 'null', null, GET(VALUE, 'c29'))) AS VARCHAR(16777216))),
	AUD_COMPANY_CD VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c30') AS VARCHAR)) = 'null', null, GET(VALUE, 'c30')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c30') AS VARCHAR)) = 'null', null, GET(VALUE, 'c30'))) AS VARCHAR(16777216))),
	AUD_LZ_FILE_NAME VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c31') AS VARCHAR)) = 'null', null, GET(VALUE, 'c31')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c31') AS VARCHAR)) = 'null', null, GET(VALUE, 'c31'))) AS VARCHAR(16777216))),
	AUD_LZ_CREATED_DTTM VARCHAR(16777216) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c32') AS VARCHAR)) = 'null', null, GET(VALUE, 'c32')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c32') AS VARCHAR)) = 'null', null, GET(VALUE, 'c32'))) AS VARCHAR(16777216))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_QAT_MAP_EZ_ENT_GLBL_RO_STG/Contract/SAP-EBS/Global/CurrentDelta/FbFssCouponsCNZTV/
refresh_on_create=false
auto_refresh=false
pattern='.*[.]tsv'
file_format={EG_RAW_DB}.ez_stage.tsv_file_format
;