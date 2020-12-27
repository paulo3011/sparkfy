import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np
from datetime import datetime
import sys

#filepath = "/home/paulo/projects/paulo3011/sparkfy/project1/data/song_data/A/A/A/TRAAAVO128F93133D4.json"
#df = pd.read_json(filepath,lines=True)
#print(df.dtypes["duration"])

def process_song_file(cur, filepath):

    # open song file
    df = pd.read_json(filepath,lines=True)
    df = df.replace(np.nan, '', regex=True)
    #df["duration"].replace({"": 0, " ": 0}, inplace=True)
    #df["artist_latitude"].replace({"": 0, " ": 0}, inplace=True)
    #df["artist_longitude"].replace({"": 0, " ": 0}, inplace=True)
    df["artist_latitude"].replace(r'^\s*$', 0.0, regex=True, inplace=True)
    df["artist_longitude"].replace(r'^\s*$', 0.0, regex=True, inplace=True)

    for ind in df.index:
        # insert song record
        song_data = (df['song_id'][ind], df['title'][ind], df['artist_id'][ind], df['year'][ind].item(), df['duration'][ind].item())
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = (df['artist_id'][ind],
                       df['artist_name'][ind], df['artist_location'][ind], df['artist_latitude'][ind], df['artist_longitude'][ind])
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    isNextSong = df['page']=="NextSong"
    df = df[isNextSong]
    # df.head(2)

    # convert timestamp column to datetime
    df['date'] = [datetime.utcfromtimestamp(x/1000) for x in df['ts']]
    df['userId'] = df['userId'].astype(int)
    # df['date'].head(2)

    for ind in df.index:
        # insert time data records
        types = (type(df['date'][ind]))
        values = (df['ts'][ind].item(),
             df['date'][ind].strftime('%H'),
             df['date'][ind].strftime('%d'),
             df['date'][ind].strftime('%U'),
             df['date'][ind].strftime('%m'),
             df['date'][ind].strftime('%Y'),
             df['date'][ind].strftime('%w'))
        # https://www.epochconverter.com/
        # https://strftime.org/
        # 1542412944796  => GMT: Saturday, 17 November 2018 00:02:24.796
        # print(values, time_table_insert)
        cur.execute(time_table_insert, values)

        # load user table
        # (user_id, first_name, last_name, gender, "level")
        types = (type(df['userId'][ind]), type(df['firstName'][ind]), type(df['lastName'][ind]), type(df['gender'][ind]), type(df['level'][ind]))
        values = (df['userId'][ind].item(), df['firstName'][ind], df['lastName'][ind], df['gender'][ind], df['level'][ind])
        try:
            cur.execute(user_table_insert, values)
        except Exception as e:
            print("Oops!", sys.exc_info())
            print(values, 'types:', types)
            raise e

        # get songid and artistid from song and artist tables
        song_filter = (df['song'][ind], df['artist'][ind], df['length'][ind])
        cur.execute(song_select, song_filter)
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            # print("songid not found:", song_select, song_filter)

        # insert songplay record
        # (start_time, user_id, "level", song_id, artist_id, session_id, "location", user_agent)
        songplay_data = (df['ts'][ind].item(),
                         df['userId'][ind].item(), df['level'][ind], songid, artistid, df['sessionId'][ind].item(), df['location'][ind], df['userAgent'][ind])
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print("Oops!", sys.exc_info())
            print(songplay_data, 'types:', get_types(songplay_data))
            raise e


def get_types(data):
    types = ()
    for d in data:
        types = (*types, type(d))
    return types


def process_data(cur, conn, filepath, func):
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
    # conn.set_session(autocommit=True)
    cur = conn.cursor()

    # path = "/home/paulo/projects/paulo3011/sparkfy/project1/"
    path = ""
    process_data(cur, conn, filepath=path + 'data/song_data', func=process_song_file)
    process_data(cur, conn, filepath=path + 'data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()