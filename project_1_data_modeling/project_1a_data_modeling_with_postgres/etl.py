import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
Reads and processes files from the two data sources 'song_data' and 'log_data', and loads them into the tables. 
Filled based on the ETL notebook.
"""

def process_song_file(cur, filepath):
    """
    Processes a song file (73 in total) and inserts its data into tables Songs and Artists
    
    Parameters:
        cur: cursor object to the database
        filepath: string to files folder
    """
    # read the song file
    df = pd.read_json(filepath, lines=True)

    # create a list of the values in the dataframe
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    # insert artist record into table Artists
    cur.execute(artist_table_insert, artist_data)
    
    # create a list of the values in the dataframe
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    # insert song record into table Songs
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """
    Processes a log file (30 in total) and inserts its data into tables Time, Users, Songplays
    
    - opens a log file
    - filters the log file by Nextsong action
    - creates the time data from the timestamp column and inserts the time data into the Time table
    - creates the user data from the log file and inserts it into the Users table
    - inserts the songplay records and adds the song_id and artist_id where possible
    
    Parameters:
        cur: cursor object to the database
        filepath: string to files folder
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # create the timedata from the tiemestamp column
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    # assign the column labels for the time dataframe 
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    # create the time dataframe using the columns labels and the time data
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    # insert time data records into the Time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # create user table data
    user_df = df[['userId','firstName','lastName','gender','level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        # selects the song_id and artist_id from the songs and artists tables where row.song = songs.title, row.artist = artists.name, row.length = songs.duration
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        # if results, then these are the song_id and artist_id
        if results:
            songid, artistid = results
        # else, add nothing
        else:
            songid, artistid = None, None

        # insert songplay record into the songplays table (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        # partially from the log data (songplay_id, start_time, user_id, level, location, user_agent), and the song data (song_id) and artist data (artist_id)
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes the data from the song and log files using the 'process_song_file' and 'process_log_file' functions respectively.
    
    Parameters: 
        cur: cursor object to the database
        conn: connection object to the database
        filepath: string to files folder
        func: file process function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Creates the sparkifydb database and extracts, transforms, and loads the data from the log files into the database.
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()