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
This script uses SQL queries from sql_queries.py to create all Fact and Dimension tables.
2. `etl.py`  
This script runs an ETL process to extract data from song_data and log_data directories and inserts it into all the tables.

#### Testing and example queries with the outcome

The workbook `test.ipynb` contains test queries for checking if the ETL process resulted in the data insert.  
The workbook `example_queries.ipynb` contains example SQL queries, which could be run by a Sparkify analysts to answer business questions.