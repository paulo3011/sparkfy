# DROP TABLES

songplay_table_drop = "drop table songplay"
user_table_drop = "drop table users"
song_table_drop = "drop table songs"
artist_table_drop = "drop table artists"
time_table_drop = "drop table time"

# CREATE TABLES

songplay_table_create = ("""
create table songplay
(
    songplay_id bigserial NOT NULL -- bigserial == default nextval('fact_songplays_id')
    ,start_time timestamp 
    ,user_id integer        REFERENCES users (user_id)
    ,level varchar(4)       -- paid or free
    ,song_id varchar(30)    REFERENCES songs (song_id)
    ,artist_id varchar(30)  REFERENCES artists (artist_id)
    ,session_id integer
    ,location varchar(60)
    ,user_agent varchar(400)
    ,PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
create table users
(
    user_id integer -- can have up to 2.147.483.647 users
    ,first_name varchar(60)
    ,last_name varchar(60)
    ,gender varchar(1)
    ,level varchar(4) -- paid or free
    ,PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
create table songs
(
    song_id varchar(30)
    ,title varchar(60)
    ,artist_id varchar(30)
    ,year smallint
    ,duration numeric(9,6) -- ex: 218.93179
    ,PRIMARY KEY (song_id)
);

""")

artist_table_create = ("""
create table artists
(
    artist_id varchar(30)
    ,name varchar(60)
    ,location varchar(60)
    ,latitude numeric(9,6)  -- ex: 35.14968
    ,longitude numeric(9,6) -- ex: -90.04892
    ,PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
create table time
(
    start_time timestamp not null -- represent the timestamp where one song was played
    ,hour smallint not null
    ,day smallint not null
    ,week smallint not null
    ,month smallint not null
    ,year smallint not null
    ,weekday smallint not null
    ,PRIMARY KEY (start_time)
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplay
(start_time, user_id, "level", song_id, artist_id, session_id, "location", user_agent)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, "level")
VALUES(%s, %s, %s, %s, %s);

""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, "year", duration)
VALUES(%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, "name", "location", latitude, longitude)
VALUES(%s, %s, %s, %s, %s);
""")


time_table_insert = ("""
INSERT INTO "time"
(start_time, "hour", "day", week, "month", "year", weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
select * from songplay limit 5
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]