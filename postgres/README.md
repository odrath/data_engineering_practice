## Data modelling with PostgreSQL
### Database schema and ETL pipeline for Sparkify.

The project involves creating a PostgreSQL database for ***Sparkify***.   

***Sparkify*** is a startup that has just released a music streaming app.  
The startup wants to analyze the data they've been collecting on songs and user activity and use it to understand what songs/artists users are listening to and when.

#### Tables and relationships
##### Fact Table
1. *songplays* - records in log data associated with song plays.  
Columns with data types and description:  
songplay_id (int, PRIMARY KEY) - unique ID of the songplay,  
start_time (timestamp) - start time of the song play,  
user_id (int) - unique ID of each user, a foreign key linking to the *time* table,   
level (varchar) - user level (free or paid), a foreign key linking to the *songs* table  
song_id (varchar) - a unique ID of each song,a foreign key linking to the *songs* table  
artist_id (varchar) - a unique ID of each artist, a foreign key linking to the *artists* table  
session_id (int) - , location, user_agent  
##### Dimension Tables
1. *users* - users in the app  
Columns: user_id, first_name, last_name, gender, level  
2. *songs* - songs in music database  
Columns: song_id, title, artist_id, year, duration  
3. *artists* - artists in music database  
Columns: artist_id, name, location, latitude, longitude  
4. *time* - timestamps of records in songplays broken down into specific units  
Columns: start_time, hour, day, week, month, year, weekday  

#### ETL Pipeline
The ETL process relies on two scripts, which have to be executed in the order below.
1. `create_tables.py`   
This script uses SQL queries from `sql_queries.py` to create all Fact and Dimension tables.
2. `etl.py`  
This script runs an ETL process to extract data from song_data and log_data directories and inserts it into all the tables.  
The script currently does not accomodate updating rows with already existing primary keys, partly due to the fact that the ETL process assumes dropping and recreating tables from scratch each time. More customised UPSERT process could be established for incremental loads, *i.e* where new song files are being added to the song_data folder on regular basis. 

#### Testing and example queries with the outcome

The workbook `test.ipynb` contains test queries for checking if the ETL process resulted in the data insert.  
The workbook `example_queries.ipynb` contains example SQL queries, which could be run by a Sparkify analysts to answer business questions.  
An example of a query and results below:  
#### Testing and example queries with the outcome

The workbook `test.ipynb` contains test queries for checking if the ETL process resulted in the data insert.  
The workbook `example_queries.ipynb` contains example SQL queries, which could be run by a Sparkify analysts to answer business questions.  
An example of a query and results below: 

> ###### When do most users listen to Sparkify?  

Sessions grouped by sessions start time (3-hour windows):   
```
WITH time_of_day AS (
    SELECT
    sp.start_time
    ,session_id
    ,CASE
      WHEN t.hour <= 3 THEN '12am - 3 am'
      WHEN t.hour <= 6 THEN '3am - 6 am'
      WHEN t.hour <= 9 THEN '6am - 9am'
      WHEN t.hour <= 12 THEN '9am - 12pm'
      WHEN t.hour <= 15 THEN '12pm - 3pm'
      WHEN t.hour <= 18 THEN '3pm - 6pm'
      WHEN t.hour <= 21 THEN '6pm - 9pm'
      ELSE '9pm - 12am' END AS session_window
FROM songplays sp
JOIN time t
ON sp.start_time = t.start_time
),
sessions AS (
    SELECT 
     MIN(start_time) AS session_start
    ,session_id
    FROM time_of_day
    GROUP BY session_id
)

SELECT COUNT(s.session_id) session_count
       ,td.session_window

FROM sessions s
JOIN time_of_day td
ON s.session_id = td.session_id
GROUP BY session_window
ORDER BY COUNT(s.session_id) DESC;
```
Results of the query:  

| session_count | session_window |
| ------------- |-------------|
| 104           | 12pm - 3pm    |
| 103           | 3pm - 6pm     |
| 101           | 6pm - 9pm     |
| 72            | 3am - 6 am    |
| 51            | 9pm - 12am    |
| 43            | 9am - 12pm    |
| 35            | 12am - 3 am   |
| 23            | 6am - 9am     |

The query can be further modified to account for the location (and the time zone) of the user.
