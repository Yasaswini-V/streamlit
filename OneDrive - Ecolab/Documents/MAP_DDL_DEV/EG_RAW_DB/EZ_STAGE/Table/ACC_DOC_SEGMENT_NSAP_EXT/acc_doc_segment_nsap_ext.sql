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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.ACC_DOC_SEGMENT_NSAP_EXT
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Finance/SAP-NSAP/Global/Delta/acc_doc_segment/Tsv/
auto_refresh=false
file_format={EG_RAW_DB}.ez_stage.EG_DEV_TSV_FF
;