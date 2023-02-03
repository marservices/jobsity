#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
import time


# In[22]:


# Setting the variables for the script

# Database Variables
db_user = 'jobsity'
db_passwd = 'jobsity'

db_schema_dw = 'TRIPS_DW'
db_schema_raw = 'TRIPS_RAW'
tb_name_raw = 'TRIPS'
tb_name_region = 'WD_REGION'
tb_name_datasource = 'WD_DATASOURCE'
tb_name_trip_group = 'WD_TRIP_GROUP'
tb_name_trips = 'WF_TRIPS'

db_url_dw = 'jdbc:mysql://'+db_user+':'+db_passwd+'@localhost/'+db_schema_dw+'?useTimezone=true&serverTimezone=UTC'
db_url_raw = 'jdbc:mysql://'+db_user+':'+db_passwd+'@localhost/'+db_schema_raw+'?useTimezone=true&serverTimezone=UTC'

# SQL to return Region Dimension Changing
sql_region = '     SELECT DISTINCT                           REGION                            FROM                                      TRIPS_RAW.TRIPS                   WHERE                                     REGION NOT IN (                           SELECT                                    REGION                            FROM                                      TRIPS_DW.WD_REGION)'

# SQL to return Datasource Dimension Changing
sql_datasource = '     SELECT DISTINCT                           DATASOURCE                        FROM                                      TRIPS_RAW.TRIPS                   WHERE                                     DATASOURCE NOT IN (                       SELECT                                    DATASOURCE                        FROM                                      TRIPS_DW.WD_DATASOURCE)'

# SQL to return Trips Group Dimension Changing
sql_trip_group = "     WITH                                                                                                                                   WF_TRIPS AS (                                                                                                                                  SELECT                                                                                                                                 DATETIME AS TRIP_DATETIME,                                                                                                             CAST(SUBSTRING_INDEX(SUBSTRING(ORIGIN_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',1) AS DECIMAL(17,15)) AS LAT_ORIGIN,                      CAST(SUBSTRING_INDEX(SUBSTRING(ORIGIN_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',-1) AS DECIMAL(17,15)) AS LON_ORIGIN,                     CAST(SUBSTRING_INDEX(SUBSTRING(DESTINATION_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',1) AS DECIMAL(17,15)) AS LAT_DEST,                   CAST(SUBSTRING_INDEX(SUBSTRING(DESTINATION_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',-1) AS DECIMAL(17,15)) AS LON_DEST               FROM                                                                                                                                       TRIPS_RAW.TRIPS TR                                                                                                             ),                                                                                                                                     TRIP_GROUPS_RAW AS (                                                                                                                       SELECT DISTINCT                                                                                                                            CAST(LAT_ORIGIN AS DECIMAL(3,1)) AS LAT_ORIGIN,                                                                                        CAST(LON_ORIGIN AS DECIMAL(3,1)) AS LON_ORIGIN,                                                                                        CAST(LAT_DEST AS DECIMAL(3,1)) AS LAT_DEST,                                                                                            CAST(LON_DEST AS DECIMAL(3,1)) AS LON_DEST,                                                                                            CASE                                                                                                                                       WHEN TIME_FORMAT(TRIP_DATETIME, '%H') BETWEEN '00' AND '06' THEN 'DAWN'                                                                WHEN TIME_FORMAT(TRIP_DATETIME, '%H') BETWEEN '07' AND '12' THEN 'MORNING'                                                             WHEN TIME_FORMAT(TRIP_DATETIME, '%H') BETWEEN '13' AND '18' THEN 'AFTERNOON'                                                           WHEN TIME_FORMAT(TRIP_DATETIME, '%H') BETWEEN '19' AND '23' THEN 'NIGHT'                                                               END AS TIME_OF_DAY                                                                                                             FROM WF_TRIPS                                                                                                                      )                                                                                                                                                                                                                                                                             SELECT                                                                                                                                     TGR.LAT_ORIGIN,                                                                                                                        TGR.LON_ORIGIN,                                                                                                                        TGR.LAT_DEST,                                                                                                                          TGR.LON_DEST,                                                                                                                          TGR.TIME_OF_DAY                                                                                                                    FROM                                                                                                                                       TRIP_GROUPS_RAW TGR                                                                                                                    LEFT JOIN TRIPS_DW.WD_TRIP_GROUP TG ON                                                                                                     TGR.LAT_ORIGIN = TG.LAT_ORIGIN                                                                                                         AND TGR.LON_ORIGIN = TG.LON_ORIGIN                                                                                                     AND TGR.LAT_DEST = TG.LAT_DEST                                                                                                         AND TGR.LON_DEST = TG.LON_DEST                                                                                                         AND TGR.TIME_OF_DAY = TG.TIME_OF_DAY                                                                                           WHERE                                                                                                                                      TG.TIME_OF_DAY IS NULL"

# SQL to return Trips Fact with its transformation
sql_trips = "            WITH                                                                                                                                 WF_TRIPS AS (                                                                                                                            SELECT                                                                                                                                   REGION_ID,                                                                                                                           DATASOURCE_ID,                                                                                                                       DATETIME AS TRIP_DATETIME,                                                                                                           CAST(SUBSTRING_INDEX(SUBSTRING(ORIGIN_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',1) AS DECIMAL(17,15)) AS LAT_ORIGIN,                    CAST(SUBSTRING_INDEX(SUBSTRING(ORIGIN_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',-1) AS DECIMAL(17,15)) AS LON_ORIGIN,                   CAST(SUBSTRING_INDEX(SUBSTRING(DESTINATION_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',1) AS DECIMAL(17,15)) AS LAT_DEST,                 CAST(SUBSTRING_INDEX(SUBSTRING(DESTINATION_COORD,8, (LENGTH(ORIGIN_COORD)-8)),' ',-1) AS DECIMAL(17,15)) AS LON_DEST,                CASE                                                                                                                                     WHEN TIME_FORMAT(DATETIME, '%H') BETWEEN '00' AND '06' THEN 'DAWN'                                                                   WHEN TIME_FORMAT(DATETIME, '%H') BETWEEN '07' AND '12' THEN 'MORNING'                                                                WHEN TIME_FORMAT(DATETIME, '%H') BETWEEN '13' AND '18' THEN 'AFTERNOON'                                                              WHEN TIME_FORMAT(DATETIME, '%H') BETWEEN '19' AND '23' THEN 'NIGHT'                                                                  END AS TIME_OF_DAY                                                                                                           FROM                                                                                                                                     TRIPS_RAW.TRIPS TR                                                                                                                   INNER JOIN TRIPS_DW.WD_REGION REG ON TR.REGION = REG.REGION                                                                          INNER JOIN TRIPS_DW.WD_DATASOURCE SR ON TR.DATASOURCE = SR.DATASOURCE                                                        )                                                                                                                                                                                                                                                                         SELECT                                                                                                                                   WF.REGION_ID,                                                                                                                        WF.DATASOURCE_ID,                                                                                                                    TG.TRIP_GROUP_ID,                                                                                                                    WF.LAT_ORIGIN,                                                                                                                       WF.LON_ORIGIN,                                                                                                                       WF.TRIP_DATETIME,                                                                                                                    WF.LAT_DEST,                                                                                                                         WF.LON_DEST                                                                                                                      FROM                                                                                                                                     WF_TRIPS WF                                                                                                                          INNER JOIN TRIPS_DW.WD_TRIP_GROUP TG ON                                                                                                  CAST(WF.LAT_ORIGIN AS DECIMAL(3,1)) = TG.LAT_ORIGIN                                                                                  AND CAST(WF.LON_ORIGIN AS DECIMAL(3,1)) = TG.LON_ORIGIN                                                                              AND CAST(WF.LAT_DEST AS DECIMAL(3,1)) = TG.LAT_DEST                                                                                  AND CAST(WF.LON_DEST AS DECIMAL(3,1)) = TG.LON_DEST                                                                                  AND WF.TIME_OF_DAY = TG.TIME_OF_DAY"

# SQL to return raw trips table structure
sql_trips_raw_schema = 'SELECT * FROM TRIPS_RAW.TRIPS WHERE 1 = 2'


# In[23]:


# Function for initialize de Spark Session with 2 threads and name 'trips-etl'
def init_spark():
    session = SparkSession.builder             .master('local[2]')             .appName('trips-etl')             .config('spark.jars', './jar_files/mysql-connector-java-8.0.13.jar')             .getOrCreate()
    return session

# Function for Spark reads the database
def spark_read(session, sql, db_url):
    dfData = session.read.format('jdbc').option("driver","com.mysql.cj.jdbc.Driver")               .option("url", db_url)               .option("query", sql)               .option("fetchsize", 20000)               .load()
    return dfData

# Function for Spark writes the datawarehouse
def spark_write(session, dfData, tbname, db_url):
    
    dfData.write.format('jdbc').option("driver","com.mysql.cj.jdbc.Driver")               .option("url", db_url)               .option("dbtable", tbname)               .option("batchsize", 20000)               .mode("append")               .save()

# Function for Spark truncates the raw trips table
def spark_truncate(session, dfData, tbname, db_url):
    
    dfData = dfData.limit(0)
    dfData.write.format('jdbc').option("driver","com.mysql.cj.jdbc.Driver")               .option("url", db_url)               .option("dbtable", tbname)               .option("truncate","true")               .mode("overwrite")               .save()


# In[24]:


# Main function of the script
def run():
    
    # Initializing of Spark Session
    session = init_spark()
    
    # Get Region dimension data change
    log_line = 'Starting of Region ETL...'
    yield(log_line)
    
    dfRegion = spark_read(session, sql_region, db_url_dw)
    
    num_rows = dfRegion.count()
    
    log_line = "Number of lines to input in dimension: " + str(num_rows)
    yield(log_line)

    start = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "Start time: " + start
    yield(log_line)
    
    # Write Region dimension data change
    spark_write(session, dfRegion, tb_name_region, db_url_dw)
    
    end = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "End time: " + start
    yield(log_line)
    
    log_line = "ETL Suceeded!!"
    yield(log_line)
    
    # Get Datasource dimension data change
    log_line = 'Starting of Datasource ETL...'
    yield(log_line)

    dfDatasource = spark_read(session, sql_datasource, db_url_dw)
    
    num_rows = dfDatasource.count()
    
    log_line = "Number of lines to input in dimension: " + str(num_rows)
    yield(log_line)

    start = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "Start time: " + start
    yield(log_line)
    
    # Write Datasource dimension data change
    spark_write(session, dfDatasource, tb_name_datasource, db_url_dw)
    
    end = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "End time: " + start
    yield(log_line)
    
    log_line = "ETL Suceeded!!"
    yield(log_line)

    # Get Trip Group dimension data change
    log_line = 'Starting of Trip Group ETL...'
    yield(log_line)
    
    dfTripGroup = spark_read(session, sql_trip_group, db_url_dw)
    
    num_rows = dfTripGroup.count()
    
    log_line = "Number of lines to input in dimension: " + str(num_rows)
    yield(log_line)

    start = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "Start time: " + start
    yield(log_line)
    
    # Write Trip Group dimension data change
    spark_write(session, dfTripGroup, tb_name_trip_group, db_url_dw)
    
    end = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "End time: " + start
    yield(log_line)
    
    log_line = "ETL Suceeded!!"
    yield(log_line)
    
    # Get Trip Fact Data from raw Trips table transformed
    log_line = 'Starting of Trip Fact ETL...'
    yield(log_line)
    
    dfTrips = spark_read(session, sql_trips, db_url_dw)
    
    num_rows = dfTrips.count()
    
    log_line = "Number of lines to input in Fact: " + str(num_rows)
    yield(log_line)

    start = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "Start time: " + start
    yield(log_line)

    # Write Trip Fact table
    spark_write(session, dfTrips, tb_name_trips, db_url_dw)
    
    end = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "End time: " + start
    yield(log_line)
    
    log_line = "ETL Suceeded!!"
    yield(log_line)
    
    # Get raw Trips table structure
    dfTripsRawSchema = spark_read(session, sql_trips_raw_schema, db_url_raw)
    
    # Truncate raw Trips table
    log_line = 'Truncating Raw Table...'
    yield(log_line)
    
    spark_truncate(session, dfTripsRawSchema, tb_name_raw, db_url_raw)
    
    log_line = 'Raw Table Truncated'
    yield(log_line)
    
    return log_line
    

