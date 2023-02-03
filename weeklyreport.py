#!/usr/bin/env python
# coding: utf-8

# In[2]:


from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from flask import Flask, request, render_template, url_for
import sqlalchemy as sa
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
import math


# In[3]:


# Setting the variables for the script

# Database Variables
db_user = 'jobsity'
db_passwd = 'jobsity'
db_schema_dw = 'TRIPS_DW'

# SQL for get trips data with week number and day of start and end of the week
sql_trips_data = "            SELECT                         REGION,                                           LAT_ORIGIN,                                          LON_ORIGIN,                                          LAT_DEST,                                            LON_DEST,                                            TRIP_DATETIME,                          WEEKOFYEAR(TRIP_DATETIME) AS TRIP_WEEK,            DATE(DATE_ADD(TRIP_DATETIME, INTERVAL -((WEEKDAY(TRIP_DATETIME)) % 7) DAY)) AS WEEK_START,                DATE(DATE_ADD(DATE_ADD(TRIP_DATETIME, INTERVAL -((WEEKDAY(TRIP_DATETIME)) % 7) DAY), INTERVAL 6 DAY)) AS WEEK_END           FROM                           WF_TRIPS WF         INNER JOIN  WD_REGION WD ON WF.REGION_ID = WD.REGION_ID"


# In[34]:


# Function to initialize the database connection 
def init_db_con(db_schema):
    
    db_url = 'mysql+pymysql://'+db_user+':'+db_passwd+'@localhost/'+db_schema

    engine = sa.create_engine(db_url)
    cnx = engine.connect()
    
    return cnx
    
# Function to get data from database
def get_db_data(sql, con):
    
    # Transform SQL in text that spark accepts
    sql = sa.text(sql)
    
    # Get de data from database as a Pandas Dataframe
    dfData = pd.read_sql(sql,con)
    
    return dfData

# Function that defines the polygon of the inputed coordinates and filter only the trips that originated in that Bounding Box
def trips_bounding_box(dfData,point1,point2,point3,point4):
    
    # Set the polygon variable with the points inputed
    polygon = Polygon([(point1), (point2), (point3), (point4)])
    
    # For each row from Dataframe, get the trips originated on bounding box
    for idx,row in dfData.iterrows():
        
        # Defines the point of the trip origin
        point = Point(row['LAT_ORIGIN'],row['LON_ORIGIN'])
        
        # Create a bool column if the trip is in or out from Bounding Box
        dfData.loc[idx, 'IN_POLYGON'] = polygon.contains(point)
    
    # Filter trips that are in the Bounding Box
    dfData = dfData[dfData['IN_POLYGON'] == True]
    
    return dfData

# Function to calculate the average trips per week in the Bounding Box
def avg_trips_polygon(dfData):
    
    # Get the count of the trips per week number
    dfAvg = dfData.groupby(['TRIP_WEEK','WEEK_START','WEEK_END'])['IN_POLYGON'].count().reset_index()
    
    # Get the average number of trips per week
    dfAvg['AVG_TRIPS'] = dfAvg['IN_POLYGON'].div(7).round(2)
    
    # Rename the column with average data
    dfAvg = dfAvg.rename(columns={'IN_POLYGON' : 'QTY_TRIPS'})
    
    return dfAvg

# Function to calculate the average trips per week in the Region inputed or all the regions
def avg_trips_region(dfData,region=None):
    
    # If a region is passed, then filter the data only in that region
    if region != None:
        dfData = dfData[dfData['REGION'].isin(region)]
    
    # Get the count of the trips per week number and region
    dfAvg = dfData.groupby(['REGION','TRIP_WEEK','WEEK_START','WEEK_END'])['LAT_ORIGIN'].count().reset_index()
    
    # Get the average number of trips per week and region
    dfAvg['AVG_TRIPS'] = dfAvg['LAT_ORIGIN'].div(7).round(2)
    
    # Rename the column with average data
    dfAvg = dfAvg.rename(columns={'LAT_ORIGIN' : 'QTY_TRIPS'})
    
    return dfAvg

# Function to build the HTML file to show the report generated
def make_html(context, dfData):
    
    # Set titles and text from the web page
    page_title_text='Average Trips by '+context
    title_text = 'Average Trips by '+context
    table_text = 'Start and End Day by Week Number'
    
    # Build the HTML file
    html = f'''
        <html>
            <head>
                <title>{page_title_text}</title>
            </head>
            <body>
                <h1>{title_text}</h1>
                <img src="{{{{url_for('static', filename='avg_report_graph.png')}}}}" width="1000" height="600" />
                <h2>{table_text}</h2>
                {dfData[['TRIP_WEEK','WEEK_START','WEEK_END']].drop_duplicates().sort_values(by='TRIP_WEEK').to_html(index=False)}
            </body>
        </html>
        '''
    
    #Write the HTML file
    with open('./main/templates/avg-report.html', 'w') as f:
        f.write(html)
    
# Function to make the graphics of the report - the context makes some diferences for the behavior
def make_plot(dfData, type_context):
    
    # Set the default context for Bounding Box
    hue = None
    context = 'Bounding Box'
    
    # Change the context if is "Region"
    if type_context == 'REGION':
        hue = 'REGION'
        context = 'Region'
    
    # Sets the style of seaborn
    sns.set_style("darkgrid")
        
    # Build the plot
    fig = sns.relplot(data=dfData, x='TRIP_WEEK', y='AVG_TRIPS', hue=hue, kind='line')
    # Change label names of the plot
    fig.set(xlabel = 'Week Of The Year', ylabel = 'Average Trips per Day')
    # Sets the interval to show in the plot
    plt.xticks(dfData['TRIP_WEEK'])
    # Saves the plot as a png image
    plt.savefig('./main/static/avg_report_graph.png')
    
    # Build the HTML file with the obtained parameters
    make_html(context, dfData)
    
    


# In[43]:


# Main function of the script
def run(region=None, coordinates=None):
    
    # Initializing of the database connection
    cnx_dw = init_db_con(db_schema_dw)
    
    # Get the data from database
    dfWFTrips = get_db_data(sql_trips_data,cnx_dw)
    
    # Tests if the context of report is the Bounding Box or Region
    if region == None:
        
        # For Bounding Box context, sets the variables
        p1 = coordinates[0]
        p2 = coordinates[1]
        p3 = coordinates[2]
        p4 = coordinates[3]
        
        # Get trips that are in the polygon
        dfTripsInPolygon = trips_bounding_box(dfWFTrips, p1, p2, p3, p4)
        
        # Get the Dataframe with the average trips per week in the polygon
        dfAvgTripsPolygon = avg_trips_polygon(dfTripsInPolygon)
        
        # Makes the plot and HTML file of the report
        make_plot(dfAvgTripsPolygon, 'BB')
        
    else:
        
        # For Region context, test if is all regions or specific regions
        if region[0] == 'all':
            
            # Get the Dataframe with the average trips per week by all regions
            dfAvgTripsRegion = avg_trips_region(dfWFTrips)
        else:
            
            # Get the Dataframe with the average trips per week by selected regions
            dfAvgTripsRegion = avg_trips_region(dfWFTrips, region=region)
        
        # Makes the plot and HTML file of the report
        make_plot(dfAvgTripsRegion, 'REGION')
        
    
    

