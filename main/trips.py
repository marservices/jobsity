#!/usr/bin/env python
# coding: utf-8

# In[2]:


from flask import Flask, request, render_template, url_for, Response
from pyspark.sql import SparkSession
import ingestion
import ETL
import weeklyreport
import os


# In[6]:


# Creating Flask app

app = Flask(__name__)#, template_folder='./templates'


# In[ ]:


@app.route("/")
def main():
    return render_template('index.html')


# In[ ]:


# Route for the Ingestion feature

@app.route("/ingestion")
def get_ingest_status_ini():
    
    return render_template('ingestion_ini.html')

@app.route("/ingestion", methods=['POST'])

def get_ingest_status():
    
    return render_template('ingestion.html')

def exec_ingest():
    
    start_msg = 'Starting Ingestion Script...\n'
    yield start_msg
    
    # Runs Ingestion script contained in ingestion.py
    log_text = ingestion.run()
    for log_line in log_text:
        log_line = log_line + '\n'
        yield log_line
    
    return log_line

@app.route('/stream_ingest')
def stream_ingest():

    return Response(exec_ingest(), mimetype='text/plain', content_type="text/event-stream")


# In[7]:


# Route for the ETL feature

@app.route("/etl")
def get_etl_status_ini():
    
    return render_template('etl_ini.html')

@app.route("/etl", methods=['POST'])

def get_etl_status():
    
    return render_template('etl.html')

def exec_etl():
    
    start_msg = 'Starting ETL Script...\n'
    yield start_msg
    
    # Runs Ingestion script contained in ETL.py
    log_text = ETL.run()
    for log_line in log_text:
        log_line = log_line + '\n'
        yield log_line
    
    return log_line

@app.route('/stream_etl')
def stream_etl():

    return Response(exec_etl(), mimetype='text/plain', content_type="text/event-stream")


# In[8]:


# Route for the Reporting feature

@app.route("/report")


# Shows HTML to wait the user's input
def wait_input_report():

    return render_template('reports.html')

# Handling the input
@app.route("/report", methods=['POST'])

def exec_report():
    
    # Get values from the buttons
    region_b = request.form.get('region_button')
    coordinates_b = request.form.get('coordinates_button')
    
    # Test if wich button was pressed
    if region_b == 'butreg':
        
        # When "Region" button is pressed get the value from the form
        region = [request.form.get('region')]
        # Process the report with the value inputed
        weeklyreport.run(region=region)
    else:
        
        # When "Coordinates" button is pressed get the values from the form
        p1 = [ float(request.form.get('1st_lat')), float(request.form.get('1st_lon')) ]
        p2 = [ float(request.form.get('2nd_lat')), float(request.form.get('2nd_lon')) ]
        p3 = [ float(request.form.get('3rd_lat')), float(request.form.get('3rd_lon')) ]
        p4 = [ float(request.form.get('4th_lat')), float(request.form.get('4th_lon')) ]
        coordinates = [p1, p2, p3, p4]
        
        # Process the report with the value inputed
        weeklyreport.run(coordinates=coordinates)
            
    return render_template('avg-report.html')

