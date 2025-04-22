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
  create or alter external table {EG_RAW_DB}.EZ_STAGE.GPS_MISSING_FILE_EXT_GPS_TEST(
	ID VARIANT AS (IFF((CAST(GET(VALUE, 'c1') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c1'))),
	TIMESTAMP VARIANT AS (IFF((CAST(GET(VALUE, 'c2') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c2'))),
	HARDWAREID VARIANT AS (IFF((CAST(GET(VALUE, 'c3') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c3'))),
	MOBILENAME VARIANT AS (IFF((CAST(GET(VALUE, 'c4') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c4'))),
	LAT VARIANT AS (IFF((CAST(GET(VALUE, 'c5') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c5'))),
	LON VARIANT AS (IFF((CAST(GET(VALUE, 'c6') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c6'))),
	HEADING VARIANT AS (IFF((CAST(GET(VALUE, 'c7') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c7'))),
	SPEED VARIANT AS (IFF((CAST(GET(VALUE, 'c8') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c8'))),
	GPSLOCK VARIANT AS (IFF((CAST(GET(VALUE, 'c9') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c9'))),
	OLD VARIANT AS (IFF((CAST(GET(VALUE, 'c10') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c10'))),
	PING VARIANT AS (IFF((CAST(GET(VALUE, 'c11') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c11'))),
	MOTION VARIANT AS (IFF((CAST(GET(VALUE, 'c12') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c12'))),
	SPEEDING VARIANT AS (IFF((CAST(GET(VALUE, 'c13') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c13'))),
	IGNITION VARIANT AS (IFF((CAST(GET(VALUE, 'c14') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c14'))),
	IGNITIONSTATUS VARIANT AS (IFF((CAST(GET(VALUE, 'c15') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c15'))),
	STREET VARIANT AS (IFF((CAST(GET(VALUE, 'c16') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c16'))),
	CITY VARIANT AS (IFF((CAST(GET(VALUE, 'c17') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c17'))),
	STATEPROVINCE VARIANT AS (IFF((CAST(GET(VALUE, 'c18') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c18'))),
	POSTALCODE VARIANT AS (IFF((CAST(GET(VALUE, 'c19') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c19'))),
	COUNTY VARIANT AS (IFF((CAST(GET(VALUE, 'c20') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c20'))),
	RSSI VARIANT AS (IFF((CAST(GET(VALUE, 'c21') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c21'))),
	SATS VARIANT AS (IFF((CAST(GET(VALUE, 'c22') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c22'))),
	TIMEZONE VARIANT AS (IFF((CAST(GET(VALUE, 'c23') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c23'))),
	SENSOR1 VARIANT AS (IFF((CAST(GET(VALUE, 'c24') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c24'))),
	SENSOR2 VARIANT AS (IFF((CAST(GET(VALUE, 'c25') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c25'))),
	DISTANCETRAVELED VARIANT AS (IFF((CAST(GET(VALUE, 'c26') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c26'))),
	INSERTEDDATE VARIANT AS (IFF((CAST(GET(VALUE, 'c27') AS VARCHAR(16777216))) = '', null, GET(VALUE, 'c27'))))
location=@{EG_RAW_DB}.EZ_STAGE.EG_TST_MAP_EZ_ENT_GLBL_RO_STG/Service/Salesforce/Global/TEST_GPS/
auto_refresh=false
pattern='.*[.]csv'
file_format=MANUAL_ALIGN_STAMP_CSV
;