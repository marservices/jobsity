
# Jobsity Challenge

## Description

To develop this solution was used MySQL database and python. I used Flask to set the API environment, PySpark to read and write the files and tables, that can handle with huge amount of data. 

The ingestion routine gets the csv file placed in the data folder and input the data in raw. After that I created an ETL routine that keeps the raw data in the table and inputs in 4 different tables: 
	- Region Dimension
	- Datasource Dimension
	- Trip Group Dimension
	- Trips Fact
	
The process, only for demonstrating purposes, is initiated by the user with a button click. But this solution is able to be automated with any tools, like a shell script that runs with cron, you can either use Azure Synapse to run the application when a file arrives at the folder. There's a lot of ways to do that. The positive part of Azure is this has a spark driver with bulk properties that can speed up the application easily.

The reports are made using SQLAlchemy to get the data and transforming in a pandas Dataframe. This Dataframe is manipulated to get the report that the user wants. The solution sets dynamic HTML that produces the web page and the graph for the context of the report. 

## Requirements

For the execution of this project some applications are needed:

1. Mysql SQL database installed
2. Python 3.9 or later
3. Installing the packages above in python environment:
	- flask
	- pandas
	- sqlalchemy
	- pyspark
	- findspark
	- shapely
	- matplotlib
	- seaborn
	- pymysql
	
4. If in windows, install a bash environment like GIT Bash

## Instructions

1. In the database with high privileges user, execute the script jobsity_create_schema
2. Sets the environment variable as HADOOP_HOME=(path to the project)\hadoop-3.2.2. For example, with you put the project in C:\Jobsity_project, the variable will be HADOOP_HOME=C:\Jobsity_project\hadoop-3.2.2
3. Add in environment variable path the item '%HADOOP_HOME%\bin'
4. Sets the environment variable PYSPARK_PYTHON='python'
5. Run the script bootstrap.sh in the project folder that will start the Flask environment
6. When the Environment is up, open the browser and go to http://127.0.0.1:8000/
7. Select the action to execute. For a first execution the process has to be:
	1. Ingestion
	2. ETL
8. Now that the database is filled up, you can execute the reports. There's two reports:
	- By Region: the default value is 'all', that gets all the regions, but you can put different regions separated by comma.
	- By coordinates: selects coordinates that represents a bounding box. For example there are default values in the report main page.
