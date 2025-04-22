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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.DC_MDM_FIELD_ORG_HIERARCHY_MDM_EXT(
	GLOBAL_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c1') AS VARCHAR)) = 'null', null, GET(VALUE, 'c1')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c1') AS VARCHAR)) = 'null', null, GET(VALUE, 'c1'))) AS VARCHAR(100))),
	GLOG_SYS_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c2') AS VARCHAR)) = 'null', null, GET(VALUE, 'c2')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c2') AS VARCHAR)) = 'null', null, GET(VALUE, 'c2'))) AS VARCHAR(100))),
	COMPANY_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c3') AS VARCHAR)) = 'null', null, GET(VALUE, 'c3')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c3') AS VARCHAR)) = 'null', null, GET(VALUE, 'c3'))) AS VARCHAR(100))),
	COUNTRY_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c4') AS VARCHAR)) = 'null', null, GET(VALUE, 'c4')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c4') AS VARCHAR)) = 'null', null, GET(VALUE, 'c4'))) AS VARCHAR(100))),
	NODE_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c5') AS VARCHAR)) = 'null', null, GET(VALUE, 'c5')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c5') AS VARCHAR)) = 'null', null, GET(VALUE, 'c5'))) AS VARCHAR(100))),
	NODE_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c6') AS VARCHAR)) = 'null', null, GET(VALUE, 'c6')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c6') AS VARCHAR)) = 'null', null, GET(VALUE, 'c6'))) AS VARCHAR(100))),
	NODE_LEVEL_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c7') AS VARCHAR)) = 'null', null, GET(VALUE, 'c7')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c7') AS VARCHAR)) = 'null', null, GET(VALUE, 'c7'))) AS VARCHAR(100))),
	PARENT_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c8') AS VARCHAR)) = 'null', null, GET(VALUE, 'c8')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c8') AS VARCHAR)) = 'null', null, GET(VALUE, 'c8'))) AS VARCHAR(100))),
	DIVISION_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c9') AS VARCHAR)) = 'null', null, GET(VALUE, 'c9')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c9') AS VARCHAR)) = 'null', null, GET(VALUE, 'c9'))) AS VARCHAR(100))),
	DIVISION_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c10') AS VARCHAR)) = 'null', null, GET(VALUE, 'c10')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c10') AS VARCHAR)) = 'null', null, GET(VALUE, 'c10'))) AS VARCHAR(100))),
	START_DT VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c11') AS VARCHAR)) = 'null', null, GET(VALUE, 'c11')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c11') AS VARCHAR)) = 'null', null, GET(VALUE, 'c11'))) AS VARCHAR(100))),
	END_DT VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c12') AS VARCHAR)) = 'null', null, GET(VALUE, 'c12')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c12') AS VARCHAR)) = 'null', null, GET(VALUE, 'c12'))) AS VARCHAR(100))),
	CURRENCY_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c13') AS VARCHAR)) = 'null', null, GET(VALUE, 'c13')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c13') AS VARCHAR)) = 'null', null, GET(VALUE, 'c13'))) AS VARCHAR(100))),
	REPORT_DISTRIBUTION_IND VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c14') AS VARCHAR)) = 'null', null, GET(VALUE, 'c14')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c14') AS VARCHAR)) = 'null', null, GET(VALUE, 'c14'))) AS VARCHAR(100))),
	ONLINE_ACCESS_IND VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c15') AS VARCHAR)) = 'null', null, GET(VALUE, 'c15')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c15') AS VARCHAR)) = 'null', null, GET(VALUE, 'c15'))) AS VARCHAR(100))),
	TERRITORY_ALIGNMENT_TYPE_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c16') AS VARCHAR)) = 'null', null, GET(VALUE, 'c16')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c16') AS VARCHAR)) = 'null', null, GET(VALUE, 'c16'))) AS VARCHAR(100))),
	ORGANIZATIONAL_TYPE_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c17') AS VARCHAR)) = 'null', null, GET(VALUE, 'c17')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c17') AS VARCHAR)) = 'null', null, GET(VALUE, 'c17'))) AS VARCHAR(100))),
	ONE_HR_NUM VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c18') AS VARCHAR)) = 'null', null, GET(VALUE, 'c18')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c18') AS VARCHAR)) = 'null', null, GET(VALUE, 'c18'))) AS VARCHAR(100))),
	USER_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c19') AS VARCHAR)) = 'null', null, GET(VALUE, 'c19')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c19') AS VARCHAR)) = 'null', null, GET(VALUE, 'c19'))) AS VARCHAR(100))),
	EMAIL_ADDR_TXT VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c20') AS VARCHAR)) = 'null', null, GET(VALUE, 'c20')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c20') AS VARCHAR)) = 'null', null, GET(VALUE, 'c20'))) AS VARCHAR(100))),
	FIRST_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c21') AS VARCHAR)) = 'null', null, GET(VALUE, 'c21')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c21') AS VARCHAR)) = 'null', null, GET(VALUE, 'c21'))) AS VARCHAR(100))),
	LAST_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c22') AS VARCHAR)) = 'null', null, GET(VALUE, 'c22')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c22') AS VARCHAR)) = 'null', null, GET(VALUE, 'c22'))) AS VARCHAR(100))),
	FULL_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c23') AS VARCHAR)) = 'null', null, GET(VALUE, 'c23')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c23') AS VARCHAR)) = 'null', null, GET(VALUE, 'c23'))) AS VARCHAR(100))),
	DESIGN_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c24') AS VARCHAR)) = 'null', null, GET(VALUE, 'c24')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c24') AS VARCHAR)) = 'null', null, GET(VALUE, 'c24'))) AS VARCHAR(100))),
	GENDER_ID_NUM VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c25') AS VARCHAR)) = 'null', null, GET(VALUE, 'c25')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c25') AS VARCHAR)) = 'null', null, GET(VALUE, 'c25'))) AS VARCHAR(100))),
	FIELD_CAM_FLAG_IND VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c26') AS VARCHAR)) = 'null', null, GET(VALUE, 'c26')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c26') AS VARCHAR)) = 'null', null, GET(VALUE, 'c26'))) AS VARCHAR(100))),
	MOD_DT VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c27') AS VARCHAR)) = 'null', null, GET(VALUE, 'c27')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c27') AS VARCHAR)) = 'null', null, GET(VALUE, 'c27'))) AS VARCHAR(100))),
	MOD_BY_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c28') AS VARCHAR)) = 'null', null, GET(VALUE, 'c28')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c28') AS VARCHAR)) = 'null', null, GET(VALUE, 'c28'))) AS VARCHAR(100))),
	CREATED_DT VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c29') AS VARCHAR)) = 'null', null, GET(VALUE, 'c29')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c29') AS VARCHAR)) = 'null', null, GET(VALUE, 'c29'))) AS VARCHAR(100))),
	CREATED_BY_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c30') AS VARCHAR)) = 'null', null, GET(VALUE, 'c30')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c30') AS VARCHAR)) = 'null', null, GET(VALUE, 'c30'))) AS VARCHAR(100))),
	CHECK_SUM_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c31') AS VARCHAR)) = 'null', null, GET(VALUE, 'c31')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c31') AS VARCHAR)) = 'null', null, GET(VALUE, 'c31'))) AS VARCHAR(100))),
	HIERARCHY_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c32') AS VARCHAR)) = 'null', null, GET(VALUE, 'c32')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c32') AS VARCHAR)) = 'null', null, GET(VALUE, 'c32'))) AS VARCHAR(100))),
	STATUS_IND VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c33') AS VARCHAR)) = 'null', null, GET(VALUE, 'c33')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c33') AS VARCHAR)) = 'null', null, GET(VALUE, 'c33'))) AS VARCHAR(100))),
	RELATION_TYPE_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c34') AS VARCHAR)) = 'null', null, GET(VALUE, 'c34')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c34') AS VARCHAR)) = 'null', null, GET(VALUE, 'c34'))) AS VARCHAR(100))),
	PARENT_GLOG_SYS_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c35') AS VARCHAR)) = 'null', null, GET(VALUE, 'c35')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c35') AS VARCHAR)) = 'null', null, GET(VALUE, 'c35'))) AS VARCHAR(100))),
	STATUS_CHANGE_DT VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c36') AS VARCHAR)) = 'null', null, GET(VALUE, 'c36')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c36') AS VARCHAR)) = 'null', null, GET(VALUE, 'c36'))) AS VARCHAR(100))),
	STATUS_CHANGE_BY_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c37') AS VARCHAR)) = 'null', null, GET(VALUE, 'c37')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c37') AS VARCHAR)) = 'null', null, GET(VALUE, 'c37'))) AS VARCHAR(100))),
	AUD_SOURCE_FILE_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c38') AS VARCHAR)) = 'null', null, GET(VALUE, 'c38')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c38') AS VARCHAR)) = 'null', null, GET(VALUE, 'c38'))) AS VARCHAR(100))),
	AUD_SOURCE_CREATED_DTTIME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c39') AS VARCHAR)) = 'null', null, GET(VALUE, 'c39')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c39') AS VARCHAR)) = 'null', null, GET(VALUE, 'c39'))) AS VARCHAR(100))),
	AUD_CDC_FLAG VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c40') AS VARCHAR)) = 'null', null, GET(VALUE, 'c40')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c40') AS VARCHAR)) = 'null', null, GET(VALUE, 'c40'))) AS VARCHAR(100))),
	AUD_CREATED_DTTM VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c41') AS VARCHAR)) = 'null', null, GET(VALUE, 'c41')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c41') AS VARCHAR)) = 'null', null, GET(VALUE, 'c41'))) AS VARCHAR(100))),
	AUD_FILE_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c42') AS VARCHAR)) = 'null', null, GET(VALUE, 'c42')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c42') AS VARCHAR)) = 'null', null, GET(VALUE, 'c42'))) AS VARCHAR(100))),
	AUD_PRIMARY_KEY_MD5_VALUE VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c43') AS VARCHAR)) = 'null', null, GET(VALUE, 'c43')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c43') AS VARCHAR)) = 'null', null, GET(VALUE, 'c43'))) AS VARCHAR(100))),
	AUD_NON_PRIMARY_KEY_MD5_VALUE VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c44') AS VARCHAR)) = 'null', null, GET(VALUE, 'c44')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c44') AS VARCHAR)) = 'null', null, GET(VALUE, 'c44'))) AS VARCHAR(100))),
	AUD_REGION_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c45') AS VARCHAR)) = 'null', null, GET(VALUE, 'c45')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c45') AS VARCHAR)) = 'null', null, GET(VALUE, 'c45'))) AS VARCHAR(100))),
	AUD_SOURCE_SYSTEM_NM VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c46') AS VARCHAR)) = 'null', null, GET(VALUE, 'c46')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c46') AS VARCHAR)) = 'null', null, GET(VALUE, 'c46'))) AS VARCHAR(100))),
	AUD_COUNTRY_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c47') AS VARCHAR)) = 'null', null, GET(VALUE, 'c47')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c47') AS VARCHAR)) = 'null', null, GET(VALUE, 'c47'))) AS VARCHAR(100))),
	AUD_SOURCE_SUBJECT_AREA VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c48') AS VARCHAR)) = 'null', null, GET(VALUE, 'c48')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c48') AS VARCHAR)) = 'null', null, GET(VALUE, 'c48'))) AS VARCHAR(100))),
	AUD_COMPANY_CD VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c49') AS VARCHAR)) = 'null', null, GET(VALUE, 'c49')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c49') AS VARCHAR)) = 'null', null, GET(VALUE, 'c49'))) AS VARCHAR(100))),
	AUD_LZ_FILE_NAME VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c50') AS VARCHAR)) = 'null', null, GET(VALUE, 'c50')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c50') AS VARCHAR)) = 'null', null, GET(VALUE, 'c50'))) AS VARCHAR(100))),
	AUD_LZ_CREATED_DTTM VARCHAR(100) AS (CAST(IFF((CAST(IFF((CAST(GET(VALUE, 'c51') AS VARCHAR)) = 'null', null, GET(VALUE, 'c51')) AS VARCHAR)) = '', null, IFF((CAST(GET(VALUE, 'c51') AS VARCHAR)) = 'null', null, GET(VALUE, 'c51'))) AS VARCHAR(100))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_QAT_MAP_EZ_ENT_GLBL_RO_STG/MDM/MDM/Global/Delta/DcMDMFieldOrgHierarchy/Tsv/
refresh_on_create=false
auto_refresh=false
pattern='.*/.*/.*[.]tsv'
file_format={EG_RAW_DB}.EZ_STAGE.eg_dev_tsv_cn_ff
;