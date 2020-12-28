# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = """
create table if not exists songplays
(
    songplay_id bigserial NOT NULL -- bigserial == default nextval('fact_songplays_id')
    ,start_time bigint
    ,user_id integer        REFERENCES users (user_id)
    ,level varchar(4)       -- paid or free
    ,song_id varchar(30)    NULL REFERENCES songs (song_id)
    ,artist_id varchar(30)  NULL REFERENCES artists (artist_id)
    ,session_id integer
    ,location varchar(60)
    ,user_agent varchar(400)
    ,PRIMARY KEY (songplay_id)
);
"""

user_table_create = """
create table if not exists users
(
    user_id integer -- can have up to 2.147.483.647 users
    ,first_name varchar(60)
    ,last_name varchar(60)
    ,gender varchar(1)
    ,level varchar(4) -- paid or free
    ,PRIMARY KEY (user_id)
);
"""

song_table_create = """
create table if not exists songs
(
    song_id varchar(30)
    ,title varchar(60)
    ,artist_id varchar(30)
    ,year smallint
    ,duration numeric(9,6) -- ex: 218.93179
    ,PRIMARY KEY (song_id)
);

"""

artist_table_create = """
create table if not exists artists
(
    artist_id varchar(30)
    ,name varchar(120)
    ,location varchar(120)
    ,latitude numeric(9,6)  -- ex: 35.14968
    ,longitude numeric(9,6) -- ex: -90.04892
    ,PRIMARY KEY (artist_id)
);
"""

time_table_create = """
create table if not exists time
(
    start_time bigint not null -- represent the timestamp where one song was played
    ,hour smallint not null
    ,day smallint not null
    ,week smallint not null
    ,month smallint not null
    ,year smallint not null
    ,weekday smallint not null
    ,PRIMARY KEY (start_time)
);
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays
(start_time, user_id, "level", song_id, artist_id, session_id, "location", user_agent)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
"""

user_table_insert = """
INSERT INTO users
(user_id, first_name, last_name, gender, "level")
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE
    SET first_name=EXCLUDED.first_name
    ,last_name=EXCLUDED.last_name
    ,gender=EXCLUDED.gender
    ,"level"=EXCLUDED."level"
"""

# artist_id	artist_latitude	artist_location	artist_longitude	artist_name	duration	num_songs	song_id	title	year
# ('ARD7TVE1187B99BFB1', '', 'California - LA', '', 'Casual', 218.93179, 1, 'SOMZWCG12A8C13C480', "I Didn't Mean To", 0)
song_table_insert = """
INSERT INTO songs
(song_id, title, artist_id, "year", duration)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO UPDATE
    SET title=EXCLUDED.title
    ,artist_id=EXCLUDED.artist_id
    ,"year"=EXCLUDED.year
    ,duration=EXCLUDED.duration
"""

artist_table_insert = """
INSERT INTO artists
(artist_id, "name", "location", latitude, longitude)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE
    SET name=EXCLUDED.name
    ,location=EXCLUDED.location
    ,latitude=EXCLUDED.latitude
    ,longitude=EXCLUDED.longitude
"""


"""
-- https://www.postgresql.org/docs/13/sql-insert.html
INSERT INTO distributors (did, dname)
    VALUES (5, 'Gizmo Transglobal'), (6, 'Associated Computing, Inc')
    ON CONFLICT (did) DO UPDATE SET dname = EXCLUDED.dname;
"""

time_table_insert = """
INSERT INTO "time"
(start_time, "hour", "day", week, "month", "year", weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO UPDATE
    SET hour=EXCLUDED.hour
    ,day=EXCLUDED.day
    ,week=EXCLUDED.week
    ,month=EXCLUDED.month
    ,year=EXCLUDED.year
    ,weekday=EXCLUDED.weekday
;
"""

# FIND SONGS

# Implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, and duration of a song.
# Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to songplay_data
# get songid and artistid from song and artist tables
song_select = """
select s.song_id, s.artist_id from songs s
join artists a on a.artist_id=s.artist_id
where s.title=%s and a.name=%s and s.duration=%s
"""

# to check if etl run ok
count_tables = """
SELECT count(0), 'songplays' as "table" FROM public.songplays
UNION all
SELECT count(0), 'songplays_with_song_id' as "table" FROM public.songplays  where song_id is not null
UNION ALL
SELECT count(0), 'artists' as "table" FROM public.artists
UNION ALL
SELECT count(0), 'artists' as "table" FROM public.songs
UNION ALL
SELECT count(0), 'users' as "table" FROM public.users
UNION ALL
SELECT count(0), 'time' as "table" FROM public.time
;
"""

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
