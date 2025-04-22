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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.CUSTOMER_PRICING_HIERARCHY_VISTEX_ECC_ET(
	SOLDTO VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c1'))),
	CSHIER1 VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c2'))),
	CSHIER1_TXT VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c3'))),
	CSHIER2 VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c4'))),
	CSHIER2_TXT VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c5'))),
	CSHIER3 VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c6'))),
	CSHIER3_TXT VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c7'))),
	CSHIER4 VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c8'))),
	CSHIER4_TXT VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c9'))),
	CSHIER5 VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c10'))),
	CSHIER5_TXT VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c11'))),
	CSHIER6 VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c12'))),
	CSHIER6_TXT VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c13'))),
	CSHIER7 VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c14'))),
	CSHIER7_TXT VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c15'))),
	CDC_FLAG VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c16'))),
	CREATED_DTTM VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c17'))),
	PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c18'))),
	NON_PRIMARY_KEY_MD5_VALUE VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c19'))),
	REGION VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c20'))),
	SOURCE_SYSTEM_NAME VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c21'))),
	COUNTRY VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c22'))),
	SUBJECT_AREA VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c23'))),
	COMPANY_CD VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c24'))),
	FILE_NAME VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c25'))),
	FILE_CREATED_DTTM VARCHAR(16777216) AS (TO_CHAR(GET(VALUE, 'c26'))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_DEV_RAW_MAP_EZ_STG/Customer/SAP-EBS/Global/Delta/0CUST_SALES_FLAT_HIER/Tsv/
auto_refresh=false
pattern='.*/.*/.*[.]tsv'
file_format=ffa_ebs_tsv_ff
COMMENT='Customer Pricing Hierarchy Pricing Vistex external table exist under eg_raw.ez_stage context mapped to ADLS Gen2 Storage. It is an external table and representing the data set available under ADLS Gen2 folder Customer/SAP-EBS/Global/Delta/0CUST_SALES_FLAT_HIER/Tsv'
;