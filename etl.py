import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """Insert song, artist data from song file into songs and artists table
    
    Get filepath is the path to song file in json format, use pandas to read content in json file, then:
    1. Extract required attributes for a song (song id, title, artist id, year, duration) and
        insert extracted data into songs table.
    2. Extract required attributes for an artist (artist id, name, location, latitude, longitude) and
        insert extracted data into artists table.
    
    Args:
        cur: cursor to the current database's connection. Type: psycopg2.extensions.cursor
        filepath: path to song json file. Type: string
        
    Raises:
        psycopg2.Error: An error occurred when inserting data into songs or artists table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Insert time, user, song played data from log file into time, users and songplays table
    
    Get filepath is the path to log file in json format, use pandas to read content in json file, then:
    1. Get data about song played by filtering NextSong value in page column.
    2. Convert timestamp (ts) column to datetime.
    3. Extract date and time attributes in datetime data, transform into DataFrame and load into time table.
    4. Extract data for each user (user id, first name, last name, gender, level) and load to users table.
    5. Get song id and artist id for each song played.
    6. Insert start_time, user id, level, song id, artist id, session id, location and user agent in each song played into songplays table.
    
    Args:
        cur: cursor to the current database's connection. Type: psycopg2.extensions.cursor
        filepath: path to log json file. Type: string
        
    Raises:
        psycopg2.Error: An error occurred when inserting data into time or users or songplays table
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts)
    
    # insert time data records
    time_data = zip(df.ts.values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(list(time_data), columns=list(column_labels))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Process data and call function to do ETL for each type of data file (songs and logs file).
    
    Get list of file paths inside filepath. For each file, call callback function and pass
    required argument (cur, current filepath) to do ETL process.
    
    Args:
        cur: cursor to the current database's connection. Type: psycopg2.extensions.cursor
        conn: connection to the database. Type: psycopg2.extensions.connection
        filepath: path to a parent file (songs or logs file path) with json format. Type: string
        func: callback function to do ETL. Type: function
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
    Connect to database, get cursor of the connection and call process_data function to do ETL process
    for each parent file path (songs and logs).
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()