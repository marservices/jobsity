#!/usr/bin/env python
# coding: utf-8

# In[7]:


from pyspark.sql import SparkSession
import time


# In[8]:


# Setting the variables for the script

# Database Variables
db_user = 'jobsity'
db_passwd = 'jobsity'
db_schema_raw = 'TRIPS_RAW'
db_table_raw = 'TRIPS'
db_url = 'jdbc:mysql://'+db_user+':'+db_passwd+'@localhost/'+db_schema_raw+'?useTimezone=true&serverTimezone=UTC'

# Path from the input file variable
trips_file = './data/trips.csv'


# In[13]:


# Function for initialize de Spark Session with 2 threads and name 'trips-ingestion'
def init_spark():
    session = SparkSession.builder             .master('local[2]')             .appName('trips-ingestion')             .config('spark.jars', './jar_files/mysql-connector-java-8.0.13.jar')             .getOrCreate()
    return session

# Function for Spark reads the input file
def spark_read(session,file):
    dfData = session.read.csv(file,header='True',schema=None)
    return dfData

# Function for ingest the data of input file in the database
def ingestion(dfData,db_url):

    
    # Open file to add lines in the log for showing in the ingestion web page
    log_line = 'Starting of Ingestion...'
    yield(log_line)
    
    log_line = 'File: ' + trips_file
    yield(log_line)
    
    # Gets number of rows for ingestion
    num_rows = dfData.count()
    
    log_line = "Number of lines to ingest: " + str(num_rows)
    yield(log_line)

    # Set initial time of the ingestion
    start_elap = time.perf_counter()
    start = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "Start time: " + start
    yield(log_line)
    
    #time.sleep(15)
    # Ingestion of data with Spark
    dfData.write.format('jdbc').option("driver","com.mysql.cj.jdbc.Driver")               .option("url", db_url)               .option("dbtable", db_table_raw)               .option("batchsize", 20000)               .mode("append")               .save()

    # Set end time of the ingestion
    end_elap = time.perf_counter()
    end = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    
    log_line = "End time: " + start
    yield(log_line)
    
    log_line = "Ingestion Suceeded!!"
    yield(log_line)
    # Transformation of the time variables for best visualization
    hours, rem = divmod(end_elap-start_elap, 3600)
    minutes, seconds = divmod(rem, 60)
    
    # Get the Elapsed time of the ingestion
    log_line = "Elapsed Time (hh:mm:ss): {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
    yield(log_line)


# In[11]:


# Main function of the script
def run():
    
    # Initializing of Spark Session
    session = init_spark()
    
    # Get data from the input file
    trips = spark_read(session,trips_file)
    
    # Ingestion of data from the input file in database
    
    
    return ingestion(trips,db_url)
    

