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
  create or alter TABLE {EG_RAW_DB}.EZ_STAGE.Y_LAST (
	EMPLOYEE_ID VARCHAR(20),
	HARDWAREID VARIANT,
	MOBILENAME VARIANT,
	START_TIME TIMESTAMP_NTZ(9),
	STOP_TIME TIMESTAMP_NTZ(9),
	START_LATITUDE VARIANT,
	START_LONGITUDE VARIANT,
	STOP_LATITUDE VARIANT,
	STOP_LONGITUDE VARIANT,
	MAX_SPEED_CALC VARIANT,
	STOP_STREET VARCHAR(16777216),
	STOP_CITY VARCHAR(16777216),
	STOP_STATEPROVINCE VARCHAR(16777216),
	STOP_POSTALCODE VARCHAR(16777216),
	STOP_COUNTY VARCHAR(16777216),
	DISTANCE_TRAVELLED FLOAT,
	TIME_BLOCK_CALCULATED_IN_TIME TIMESTAMP_NTZ(9),
	TIME_BLOCK_CALCULATED_IN_TIME_LEAD TIMESTAMP_NTZ(9),
	TIME_BLOCK_CALCULATED_OUT_TIME TIMESTAMP_NTZ(9),
	TIME_BLOCK_CALCULATED_OUT_TIME_LEAD TIMESTAMP_NTZ(9),
	MOVING_TIME NUMBER(24,6),
	IDLE_TIME NUMBER(24,6),
	INSIDE_TIME_BLOCK_FLAG VARCHAR(1),
	INSIDE_TIME_BLOCK_FLAG_LEAD VARCHAR(1),
	INSIDE_TIME_BLOCK_FLAG_LEAD_FOR_IDLE VARCHAR(1),
	START_TIME_LEAD_FOR_IDLE TIMESTAMP_NTZ(9),
	STOP_TIME_LAG_FOR_IDLE TIMESTAMP_NTZ(9),
	TIME_BLOCK_CALCULATED_IN_TIME_LEAD_FOR_IDLE TIMESTAMP_NTZ(9),
	STOP_TIME_ORIGINAL TIMESTAMP_NTZ(9)
);