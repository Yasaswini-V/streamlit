-------------------------------------------------------------------------------
  -- Author       : Azure Devops Bot
  -- Project      : 
  -- Database     : EG_RAW_DB
  -- Schema       : LZ_STAGE
  -- Object Type  : Table
  -------------------------------------------------------------------------------
  -- Change Log 
  -------------------------------------------------------------------------------
  -- Version |Modified Date |Engineer Name                 |Change description
  -------------------------------------------------------------------------------
  -- v.1.0   |2025-04-22    |Azure Devops Bot               |Initial DDL extracted from snowflake 
  --
  -------------------------------------------------------------------------------
  create or alter external table {EG_RAW_DB}.LZ_STAGE.MOBILEDISTANCETRAVELED_GPS_EXT(
	XMLDATA VARIANT AS (GET(XMLGET(VALUE, 'soap:Body', 0), '$')))
location=@{EG_RAW_DB}.LZ_STAGE.EG_DEV_MAP_LZ_ENT_RO_STG/NexTraq/Global/GPS/GPSTrack/History/
auto_refresh=false
pattern='.*GPSTrack_.*[.]xml'
file_format=(TYPE=xml)
;