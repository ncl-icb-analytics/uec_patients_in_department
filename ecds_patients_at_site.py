#Imports
import json
import pandas as pd
import ncl_sqlsnippets as snips
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from os import getenv



####Import env settings
def import_settings():
    load_dotenv(override=True)

    return {

        #Window to process
        "DATE_WINDOW": getenv("DATE_WINDOW"),

        #Site code array
        "SITES": json.loads(getenv("SITES")),

        #SQL Server Details
        "SQL_ADDRESS": getenv("SQL_ADDRESS"),
        "SQL_DATABASE": getenv("SQL_DATABASE"),
        "SQL_SCHEMA": getenv("SQL_SCHEMA"),
        "SQL_TABLE": getenv("SQL_TABLE")
    }

settings = import_settings()



###Establish processing window
print("Establishing processing window")

#Takes the input DATE_WINDOW and returns the date_start as a date type variable
def process_date_window(window, date_end):
    #If a number is given then assume it is in terms of days
    if isinstance(window, int):
        return date_end - timedelta(days=window-1)
    
    #If window is written:
    input_window = window.split(" ")

    #Sanitise input
    if len(input_window) != 2:
        raise Exception(f"The window type {window} is not formatted correctly.")
    
    if input_window[1].endswith('s'):
        input_window[1] = input_window[1][:-1]

    #Process window value to get date_start
    if input_window[1] == "day":
        return date_end - timedelta(days = int(input_window[0]) - 1)
    
    elif input_window[1] == "week":
        return date_end - timedelta(days = (int(input_window[0]) * 7) - 1)
    
    elif input_window[1] == "month":
        return date_end - relativedelta(months = int(input_window[0]))
    
    elif input_window[1] == "year":
        return date_end - relativedelta(years = int(input_window[0]))
    
    else:
        raise Exception(f"The window type {window.split(' ')[1]} is not supported.")
    
#Get the date at the start of the window
query_date_start = process_date_window(settings["DATE_WINDOW"], datetime.now())

#Get the previous monday from the window start so all weeks are complete
query_week_start = query_date_start - pd.to_timedelta((query_date_start.weekday()) % 7, unit='D')



###Import raw ECDS Data
print("Importing the raw ECDS Data")

#Import ECDS Data
engine = snips.connect('PSFADHSSTP01.AD.ELC.NHS.UK:1460','NCL')

#Read SQL script
with open('./SQL/EDCS_ambulance_patients_on_site.sql', 'r') as file:
    query_ecds_base = file.read()

#Append the window condition to the query
query_ecds = query_ecds_base + f"\n AND [attendance.arrival.date] >= '{query_week_start}'"

#Run query and store in data frame
df_raw = snips.execute_sfw(engine, query_ecds)



###Clean the raw input
print("Processing the raw input")

#Clean copy of raw input
df_clean = df_raw.copy()

#Convert date columns to date types (from strings)
df_clean['arrival_date'] = df_clean['arrival_date'].astype('datetime64[s]')
df_clean['departure_date'] = df_clean['departure_date'].astype('datetime64[s]')

#Remove future rows (departure dates that occur in the future, data quality issue)
#Current date
current_date = datetime.now()

#Remove all rows with a departure date in the future
df_clean = df_clean[df_clean['departure_date'] <= current_date]



###Create the hour table (The data but with 1 row per hour on site)

#Get the hour of a date (As a 2 length char)
def get_hour(time_value):
    return int(time_value[0:2])

#Split a single ECDS patient row into 1 row per hour spent on sitelean
def hours_in_site(pat):

    #Range for iteration
    start_date = pat['arrival_date']
    start_hour = get_hour(pat['arrival_time'])
    end_date = pat['departure_date']
    end_hour = get_hour(pat['departure_time'])

    site = pat['site_code']

    #Inititialise for iteration
    current_date = start_date
    current_hour = start_hour

    #Array for new rows
    hours = []

    #Flag to mark the first hour spent as an 'arrival' so I can sum 'arrivals' during aggregation
    arrival_marked = False

    #Iterate each hour between the start and end date
    while (current_date < end_date) or ((current_date == end_date) and current_hour <= end_hour):

        if arrival_marked:
            hours.append({"date": current_date, "hour": current_hour, "site_code": site, "patients":1, "arrivals":0})
        else:
            hours.append({"date": current_date, "hour": current_hour, "site_code": site, "patients":1, "arrivals":1})
            arrival_marked = True

        #Code to update hour each iteration
        if current_hour == 23:
            current_hour = 0
            current_date += timedelta(days=1)
        else:
            current_hour += 1

    return hours  

# Iterate through the rows using iterrows()
all_hours = []
for index, row in df_clean.iterrows():
    all_hours += hours_in_site(row)

df_hours = pd.DataFrame(all_hours)



###Aggregate the hours table by hours and site
print("Aggregating the data")

#Aggregate the hour table to get patients and arrivals at each site for each hour
df_hours_agg = df_hours.groupby(['date', 'hour', 'site_code']).agg({'patients':'count', 'arrivals':'sum'}).reset_index()

#Get the most recent week starting day (Previous Monday)
most_recent_weekstart = (current_date - timedelta(days=((current_date.weekday() - 0) % 7) + 7)).strftime('%Y-%m-%d')
#Filter out any incomplete weeks (the latest week)
df_hours_agg = df_hours_agg[df_hours_agg['date'] < most_recent_weekstart]

#Add the date_weekstarting (Monday) and date_weekending (Sunday) columns to the dataframe
df_hours_agg['date_weekstarting'] = df_hours_agg['date'] - pd.to_timedelta((df_hours_agg['date'].dt.dayofweek) % 7, unit='D')
df_hours_agg['date_weekending'] = df_hours_agg['date'] - pd.to_timedelta((df_hours_agg['date'].dt.dayofweek) % 7 - 6, unit='D')



###Aggregate the data into weeks
df_weeks = df_hours_agg.copy()

#Add the completeness column
df_weeks['completeness'] = 1
#Group by weeks and site
df_weeks = df_weeks.groupby(
    ['date_weekstarting', 'date_weekending', 'site_code']
    ).agg({'patients': 'sum', 'arrivals':'sum', 'completeness':'count'}).reset_index()

#Create mean versions of existing metrics by dividing by 24*7 (hours in a week)
df_weeks['patients_mean'] = df_weeks['patients'] / 168
df_weeks['arrivals_mean'] = df_weeks['arrivals'] / 168
df_weeks['completeness'] = df_weeks['completeness'] / 168

#Add fin_year and month columns
df_weeks['fin_year'] = df_weeks['date_weekstarting'].apply(
    lambda x: str(int(x.to_period('Q-MAR').qyear) - 1) + '-' + str(int(x.to_period('Q-MAR').qyear) - 2000)
    )
df_weeks['month'] = df_weeks["date_weekstarting"].dt.strftime("%b")

#Remove excess columns
df_weeks = df_weeks[
    ["date_weekstarting", "date_weekending", "fin_year", "month", "site_code", "patients_mean", "arrivals_mean", "completeness"]
    ]



###Upload result
print("Uploading the result")

#Connect to the database
engine = snips.connect("PSFADHSSTP01.AD.ELC.NHS.UK,1460", "Data_Lab_NCL_Dev")

#Check if the target table exists and create it if not
if not snips.table_exists(engine, settings["SQL_TABLE"], settings["SQL_SCHEMA"]):
    #Build create table query
    full_table_path = f"[{settings['SQL_DATABASE']}].[{settings['SQL_SCHEMA']}].[{settings['SQL_TABLE']}]"
    create_query_header = f"CREATE TABLE {full_table_path} (\n"

    with open("./SQL/create_template.sql", "r") as file:
        create_query_base = file.read()

    create_query = create_query_header + create_query_base.split("\n", 1)[1]

    session = snips.execute_query(engine, create_query)

#Else code to delete overlapping data
else:
    #Build delete data query
    earliest_week_start = df_weeks["date_weekstarting"].min()
    delete_query = f"DELETE FROM [Data_Lab_NCL_Dev].[JakeK].[uec_patients_in_department_dev] WHERE date_weekstarting >= '{earliest_week_start}';"

    #Delete old data
    session = snips.execute_query(engine, delete_query)

#Upload result
snips.upload_to_sql(df_weeks, engine, settings["SQL_TABLE"], settings["SQL_SCHEMA"], replace=False, chunks=250) 