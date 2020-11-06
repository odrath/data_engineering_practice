# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                             songplay_id int PRIMARY KEY
                             ,start_time timestamp
                             ,user_id int
                             ,level varchar
                             ,song_id varchar
                             ,artist_id varchar
                             ,session_id int
                             ,location varchar
                             ,user_agent varchar
                             );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id int PRIMARY KEY
                        ,first_name varchar
                        ,last_name varchar
                        ,gender varchar(1)
                        ,level varchar
                        );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                        song_id varchar PRIMARY KEY
                        ,title varchar
                        ,artist_id varchar
                        ,year int
                        ,duration numeric
                        );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                        artist_id varchar PRIMARY KEY
                        ,name varchar
                        ,location varchar
                        ,latitude numeric
                        ,longtitude numeric
                        );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time timestamp without time zone
                        ,hour smallint
                        ,day smallint
                        ,week smallint
                        ,month smallint
                        ,year smallint
                        ,weekday smallint
                        );
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays
                             (songplay_id
                              ,start_time
                              ,user_id
                              ,level
                              ,song_id
                              ,artist_id
                              ,session_id
                              ,location
                              ,user_agent) 
                              VALUES (%s, %s, %s,%s,%s,%s,%s,%s,%s)
                              ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""INSERT INTO users
                             (user_id
                              ,first_name
                              ,last_name
                              ,gender
                              ,level) 
                              VALUES (%s, %s, %s,%s,%s)
                              ON CONFLICT (user_id) DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs
                             (song_id
                              ,title
                              ,artist_id
                              ,year
                              ,duration) 
                              VALUES (%s, %s, %s,%s,%s)
                              ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists
                             (artist_id
                              ,name
                              ,location
                              ,latitude
                              ,longtitude) 
                              VALUES (%s, %s, %s,%s,%s)
                              ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""INSERT INTO time
                             (start_time
                              ,hour
                              ,day
                              ,week
                              ,month
                              ,year
                              ,weekday) 
                              VALUES (%s, %s,%s, %s,%s,%s,%s)
""")

# FIND SONGS  song ID and artist ID based on the title, artist name, and duration of a song

song_select = (""" SELECT s.song_id, s.artist_id       
                   FROM songs s
                   LEFT JOIN artists a
                   ON s.artist_id = a.artist_id
                   WHERE (s.title = %s AND s.duration = %s) OR a.name = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]