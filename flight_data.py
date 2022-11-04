import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import streamlit as st
import altair as alt
import os

# Creating the pattern for the sqlalchemy engine to connect to snowflake database
# SQLAlchemy uses the connection string - 'snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
url = URL(
                user = os.getenv('USER'),
                password = os.getenv('PASSWORD'),
                account= os.getenv('ACCOUNT'),
                warehouse= os.getenv('WAREHOUSE'),
                database=os.getenv('DATABASE'),
                schema= os.getenv('SCHEMA')
                )  
  

# Get the data from the table
query1 = '''
    select  * from final_schedule_table where departure_country_code = 'US' 
AND arrival_country_code = 'US' AND departure_port_code IN ('JFK','CLT','ORD')
AND service_desc = 'Passenger'
'''
@st.cache(allow_output_mutation=True)
def get_data_airline(query1):
    # describes how to talk to a specific kind of database/DBAPI combination.
    engine = create_engine(url)
    # The connection to the DBAPI is made when connect method is called
    conn = engine.connect()   
    # Load the data into the dataframe
    df_data = pd.read_sql(query1, conn)
    conn.close()
    engine.dispose()
    return df_data

df_data = get_data_airline(query1)
# Combine airline code and airline to ease out the understanding of airline code
df_data['Airline_Code_Name'] = np.where(df_data['airline_name'].isnull()\
                                        , df_data['airline_code']\
                                        , df_data['airline_code'] + '-' + df_data['airline_name'] )

# Concatenate departure code and name
df_data['departure'] = np.where(df_data['departure_airport_name'].isnull()\
                                        , df_data['departure_port_code']\
                                        , df_data['departure_port_code'] + '-' + df_data['departure_airport_name'] )  

# Concatenate arrival code and name
df_data['arrival'] = np.where(df_data['arrival_airport_name'].isnull()\
                                        , df_data['arrival_port_code']\
                                        , df_data['arrival_port_code'] + '-' + df_data['arrival_airport_name'] )                                                                               

# get the descriptions of the inflight services 
query2 = '''
    select  * from INFLIGHT_SVC_DES
'''
@st.cache(allow_output_mutation=True)
def get_data_inflight(query2):
    # describes how to talk to a specific kind of database/DBAPI combination.
    engine = create_engine(url)
    # The connection to the DBAPI is made when connect method is called
    conn = engine.connect()   
    # Load the data into the dataframe
    df_inflight = pd.read_sql(query2, conn)
    conn.close()
    engine.dispose()
    return df_inflight

data_inflight = get_data_inflight(query2)

st.sidebar.header("DSBA-5122 Streamlit App")
st.sidebar.header("Select your data")
# Airline selection
airline = st.sidebar.selectbox("Select airline",sorted(set(df_data['Airline_Code_Name'])))

airline_code = airline.split("-")[0]
# Date selection
date = st.sidebar.selectbox("Select the date",sorted(set(df_data['flight_date'])))
# Departure Port
dep_list = set(df_data[(df_data['airline_code'] == airline_code) & \
                                (df_data['flight_date'] == date)]['departure'].values)

departure = st.sidebar.selectbox("Select the departure airport",sorted(dep_list))

dep_code = departure.split("-")[0]

# Arrival Port
# Do not list the arrival port which has already been selected as the departure port
if departure != '':
    arrival_list = set(df_data[(df_data['departure_port_code'] == dep_code) &\
                                (df_data['airline_code'] == airline_code) & \
                                (df_data['flight_date'] == date)]['arrival'].values)
    # arrival_list.remove(departure)
    arrival = st.sidebar.selectbox("Select the arrival airport",sorted(arrival_list))

# Get the codes from the data selected
air_code = arrival.split("-")[0]

st.header("Detailed Flight data based on your selection:")
st.write(df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)])


total_stops = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]['total_stops_in_route'].values

nonstop_cnt= 0

val_idx = []
# Check if all the values in list is 0 --> direct flight
for stop in total_stops:
    if stop == 0:
        nonstop_cnt += 1
    else:        
        val_idx.append(total_stops.index)

st.header("Stops Information:")

if nonstop_cnt != 0:
    st.write(f'You have **{nonstop_cnt} non-stop** flights from **{departure}** to **{arrival}** on {date} you have selected')
if len(val_idx) !=0 :
    st.write(f'You have **{len(val_idx)}** flights with stops from **{departure}** to **{arrival}** on {date} you have selected')

route_path = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]\
                            ['route_path'].values  

full_route = ''
for path in route_path:
    if full_route == '':
        full_route = path
    else:
        full_route = full_route + ',' + path
        
st.write(f'The route of the airline will be:\n**{full_route}**')                         

st.header("Distance & Duration Information (Index as flight number")
df_dt_dur = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]\
                            [['distance','elapsed_time','flight_number']].copy()
df_dt_dur.rename(columns = {'elapsed_time':'duration'}, inplace=True) 
df_dt_dur.set_index('flight_number', inplace=True)
st.write(df_dt_dur)                           

st.header('**Seats Information**')

first_class_seats = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]\
                        .groupby('flight_date')['first_class_seats'].sum()[0]
# st.write(f'The first class seats available on this path is :- {first_class_seats}') 

business_class_seats = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]\
                        .groupby('flight_date')['business_class_seats'].sum()[0]
# st.write(f'The business class seats available on this path is :- {business_class_seats}')

premium_economy_class_seats = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]\
                        .groupby('flight_date')['premium_economy_class_seats'].sum()[0]

economy_plus_class_seats = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]\
                        .groupby('flight_date')['economy_plus_class_seats'].sum()[0] 

economy_class_seats = df_data[(df_data['airline_code'] == airline_code) & (df_data['flight_date'] == date) \
                        & (df_data['departure_port_code'] == dep_code) & (df_data['arrival_port_code'] == air_code)]\
                        .groupby('flight_date')['economy_class_seats'].sum()[0]                                               

df_seats_info = pd.DataFrame(
    {
        'seat_type' : ['first_class_seats','business_class_seats','premium_economy_class_seats','economy_plus_class_seats','economy_class_seats'],
        'total_values' : [round(first_class_seats), round(business_class_seats), round(premium_economy_class_seats),\
                            round(economy_plus_class_seats), round(economy_class_seats) ]
    })

# st.write(df_seats_info)

bar_chart = alt.Chart(df_seats_info, height = 300, title = f'Seats info from {departure} to {arrival}').mark_bar(color = 'purple')\
            .encode(
                    alt.X('total_values', title = 'Total number of seats'),
                    alt.Y('seat_type', title = 'Type of seats', sort = '-x'),
                    # tooltip = ['total_values']
                    tooltip=[alt.Tooltip('total_values', title='Total Seats')]
                    )
st.altair_chart(bar_chart, use_container_width=True)
