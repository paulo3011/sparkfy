import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS ;"
staging_songs_table_drop = "DROP TABLE IF EXISTS ;"

songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"

user_table_drop = "DROP TABLE IF EXISTS ;"
song_table_drop = "DROP TABLE IF EXISTS ;"
artist_table_drop = "DROP TABLE IF EXISTS ;"
time_table_drop = "DROP TABLE IF EXISTS ;"

# CREATE TABLES

staging_events_table_create= ("""
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
    songplay_id bigint IDENTITY(0,1) NOT NULL
    ,start_time bigint
    ,user_id integer        REFERENCES users (user_id)
    ,level varchar(4)       -- paid or free
    ,song_id varchar(30)    NULL REFERENCES songs (song_id)
    ,artist_id varchar(30)  NULL REFERENCES artists (artist_id)
    ,session_id integer
    ,location varchar(60)
    ,user_agent varchar(400)
    ,PRIMARY KEY (songplay_id)
)
DISTSTYLE KEY
DISTKEY (song_id)
COMPOUND SORTKEY (start_time, song_id, user_id, session_id)
;

COMMENT ON TABLE fact_songplays IS 'records in event data associated with song plays i.e. records with page NextSong';
""")

"""
SAMPLE QUESTIONS:

0. What songs users are listening to? (Currently, they don't have an easy way to query their data)
-> SORTKEY (song_id, user_id)

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

-> SORTKEY (song_id, user_id)

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

-> SORTKEY (song_id, user_id, session_id)

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

-> SORTKEY (song_id, user_id, session_id)
"""

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id integer
    ,first_name varchar(60)
    ,last_name varchar(60)
    ,gender varchar(1)
    ,level varchar(4)
    ,PRIMARY KEY (user_id)
);

COMMENT ON TABLE dim_users IS 'users in the app';
COMMENT ON COLUMN dim_users.level IS 'paid or free';
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id varchar(30)
    ,title varchar(60)
    ,artist_id varchar(30)
    ,year smallint
    ,duration numeric(9,6)
    ,PRIMARY KEY (song_id)
);

COMMENT ON TABLE dim_songs IS 'songs in music database';
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id varchar(30)
    ,name varchar(120)
    ,location varchar(120)
    ,latitude numeric(9,6)
    ,longitude numeric(9,6)
    ,PRIMARY KEY (artist_id)
);

COMMENT ON TABLE dim_artists IS 'artists in music database';
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time bigint not null
    ,hour smallint not null
    ,day smallint not null
    ,week smallint not null
    ,month smallint not null
    ,year smallint not null
    ,weekday smallint not null
    ,PRIMARY KEY (start_time)
);

COMMENT ON TABLE dim_time IS 'timestamps of records in songplays broken down into specific units';
COMMENT ON COLUMN dim_time.start_time IS 'represent the timestamp where one song was played';
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
