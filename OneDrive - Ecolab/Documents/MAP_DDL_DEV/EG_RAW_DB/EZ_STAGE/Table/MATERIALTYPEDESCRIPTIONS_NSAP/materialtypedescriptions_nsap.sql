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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.MATERIALTYPEDESCRIPTIONS_NSAP (
	CLIENT VARCHAR(16777216) COMMENT 'Client',
	LANGUAGE VARCHAR(16777216) COMMENT 'Language Key',
	MATERIAL_TYPE VARCHAR(16777216) COMMENT 'Material Type',
	SOURCE_SYS VARCHAR(16777216) COMMENT 'Source System',
	MATTYPE_DESCR VARCHAR(16777216) COMMENT 'Description of material type',
	FILE_NAME VARCHAR(16777216) COMMENT 'Ingest file name',
	CREATED_DTTM TIMESTAMP_NTZ(9) COMMENT 'File created timestamp',
	LAST_UPDT_DTTM TIMESTAMP_NTZ(9) COMMENT 'Last updated timestamp',
	JOBID VARCHAR(16777216) COMMENT 'Job identifier',
	FILE_CREATED_DTTM NUMBER(38,0) COMMENT 'File created timestamp',
	STATUS VARCHAR(16777216) COMMENT 'Status'
)COMMENT='Material Type Descriptions'
;