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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.GPS_MISSING_FILE_EXT_F (
	ID VARIANT,
	TIMESTAMP VARIANT,
	HARDWAREID VARIANT,
	MOBILENAME VARIANT,
	LAT VARIANT,
	LON VARIANT,
	HEADING VARIANT,
	SPEED VARIANT,
	GPSLOCK VARIANT,
	OLD VARIANT,
	PING VARIANT,
	MOTION VARIANT,
	SPEEDING VARIANT,
	IGNITION VARIANT,
	IGNITIONSTATUS VARIANT,
	STREET VARIANT,
	CITY VARIANT,
	STATEPROVINCE VARIANT,
	POSTALCODE VARIANT,
	COUNTY VARIANT,
	RSSI VARIANT,
	SATS VARIANT,
	TIMEZONE VARIANT,
	SENSOR1 VARIANT,
	SENSOR2 VARIANT,
	DISTANCETRAVELED VARIANT,
	INSERTEDDATE TIMESTAMP_NTZ(9)
);