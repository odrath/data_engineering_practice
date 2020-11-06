import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np
# the extension below handles issues connected to psycopg2 having problems with understanding numpy data types
from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def process_song_file(cur, filepath):
    """
    - Processes a json file from the song_data folder to extract information about the song and artist.
    - Inserts the relevant data into the songs and artists dimensional tables.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].iloc[0].values.tolist()
    cur.execute(song_table_insert, song_data)
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].iloc[
        0].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Processes a json file from the log_data folder to extract information about the songplay session and user.
    - Inserts the relevant data into the users, time and songplays fact table.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_dict = {column_labels[i]: time_data[i] for i in range(len(column_labels))}
    time_df = pd.DataFrame.from_dict(time_dict, orient='columns')

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset=['userId'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length, row.artist))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
        index, pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location,
        row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Gets all the files from the specified directory.
    - Iterates over files and processes them to insert rows into database tables.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        try:
            func(cur, datafile)
            print(f"File {i}/{num_files} processed.")
        except psycopg2.Error as e:
            print(f"File {i}/{num_files} not processed.")
            print(e)


def main():
    """
    - Creates a database connection.
    - Runs an ETL processs for the Sparkify database.
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
        conn.set_session(autocommit=True)
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
        conn.close()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the DB")
        print(e)


if __name__ == "__main__":
    main()