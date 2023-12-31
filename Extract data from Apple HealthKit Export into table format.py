#import libraries

import pandas as pd
import xml.etree.ElementTree as ET
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas_gbq
import pytz
import google.auth 
credentials, project = google.auth.default()
os.chdir('INSERT YOUR FILE PATH HERE')

##Extracts data from the Apple HealthKit XML export

tree = ET.parse('export.xml')
root = tree.getroot()

df_cols = ["type", "sourceName", "unit", "creationDate", "startDate", "endDate", "value"]
rows = []

for node in root:
    if node.tag == "Record":
        record_type = node.attrib.get("type")
        source_name = node.attrib.get("sourceName")
        unit = node.attrib.get("unit")
        creation_date = node.attrib.get("creationDate")
        start_date = node.attrib.get("startDate")
        end_date = node.attrib.get("endDate")
        value = node.attrib.get("value")
        rows.append({"type": record_type, "sourceName": source_name, "unit": unit, "creationDate": creation_date, "startDate": start_date, "endDate": end_date, "value": value})


out_df = pd.DataFrame(rows, columns=df_cols)

##cleans up the type field to strip off the Health Kit Labeling

out_df2 = out_df.copy()
out_df2['type'] = out_df['type'].str.replace('HKQuantityTypeIdentifier', '')
out_df2['endDate'] = pd.to_datetime(out_df2['endDate'])
out_df2['key_field'] = out_df2['sourceName'] + ['_'] + out_df2['type']

# Get the current date
current_date = pd.Timestamp.now(tz='UTC-05:00')

# Calculate the date 90 days ago
date_90_days_ago = current_date - pd.Timedelta(days=90)

#pulls last 90 days of data
out_df3 = out_df2[out_df2['endDate'] >= date_90_days_ago]

#export Apple Watch ActiveEnergyBurned ONLY. You can modify this to export any metrics you want

out_df_AppleWatch = out_df3[out_df3['key_field'].isin(['Apple Watch_ActiveEnergyBurned'])]
out_df_AppleWatch['value'] = out_df_AppleWatch['value'].astype(float)

#Aggregate data to one weekly date (every Monday)
#This is an optional step in order to have a weekly view. It can be de-aggregated in case of wanting to see more granularly

out_df_AppleWatch_Grouped = out_df_AppleWatch['endDate'] = pd.to_datetime(out_df_AppleWatch['endDate']) - pd.to_timedelta(7, unit='d')
out_df_AppleWatch_Grouped = out_df_AppleWatch.groupby(['key_field','type','sourceName','unit', pd.Grouper(key='endDate', freq='W-MON')])['value'].sum().reset_index().sort_values('endDate')
out_df_AppleWatch_Grouped

#In the event of wanting the push the data into BigQuery, you can use this snippet
#Find setup data here: https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries

# Construct a BigQuery client object.
client = bigquery.Client()

# Set the project ID and table ID
project_id = "your project id"
table_id = "your table id"

# Load the DataFrame to BigQuery
pandas_gbq.to_gbq(out_df_AppleWatch_Grouped, table_id, project_id=project_id, if_exists="replace")