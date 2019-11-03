import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info and used to populate the songs and artists dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """   

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id","title","artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=="NextSong"]
    
    # convert timestamp column to datetime and get the attribute of the date (week, day, months, ...)
    df["datetime"] = df.ts.apply(lambda num: datetime.fromtimestamp(num/1000))
    df["dt_year"] = df["datetime"].dt.year
    df["dt_month"] = df["datetime"].dt.month
    df["dt_day"] = df["datetime"].dt.day
    df["dt_hour"] = df["datetime"].dt.hour
    df["dt_weekday"] = df["datetime"].dt.weekday
    df["dt_week"] = df["datetime"].dt.week

    # insert time data records
    time_df = df[["datetime","dt_hour","dt_day","dt_week","dt_month","dt_year","dt_weekday"]]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data=[df.datetime[index],str(df.userId[index]),df.level[index],songid,artistid,str(df.sessionId[index]),df.location[index],df.userAgent[index]]

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function can be used to process the files contained in a filepath. 
    The way the files are processed are specified in the function "fun".

    Arguments:
        cur: the cursor object. 
        conn: connection to the database
        filepath: file path to process
        fun: function to use to process the filepath

    Returns:
        None
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()