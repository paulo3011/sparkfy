class SqlQueries:
    songplay_table_insert = ("""
    DELETE
    FROM
        fact_songplays
    USING dim_time
    WHERE
        dim_time.start_time = fact_songplays.start_time
        AND dim_time.month = {{execution_date.strftime("%m")}}
        AND dim_time.year = {{execution_date.strftime("%Y")}}
    """, """
    INSERT
        INTO
        fact_songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, start_date)
    SELECT
        events.ts as start_time,
        events.userid,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.sessionid,
        events.location,
        events.useragent,
        events.start_date
    FROM
        (
        SELECT
            TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS start_date, *
        FROM
            stage_events
        WHERE
            page = 'NextSong') events
    LEFT JOIN stage_songs songs ON
        events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration;
    """)

    user_table_insert = ("TRUNCATE TABLE dim_user;", """
    INSERT
        INTO
        dim_user (user_id, first_name, last_name, gender, "level")
    SELECT
        distinct userid,
        firstname,
        lastname,
        gender,
        level
    FROM
        stage_events
    WHERE
        page = 'NextSong';
    """)

    song_table_insert = ("TRUNCATE TABLE dim_song;", """
    INSERT
        INTO
        dim_song (song_id, title, artist_id, "year", duration)
    SELECT
        distinct song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        stage_songs;
    """)

    artist_table_insert = ("TRUNCATE TABLE dim_artist;", """
    INSERT
        INTO
        dim_artist (artist_id, "name", location, latitude, longitude)
    SELECT
        distinct artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        stage_songs;
    """)

    time_table_insert = ("TRUNCATE TABLE dim_time;", """
    INSERT
        INTO
        dim_time (start_time, "hour", "day", week, "month", "year", weekday)
    SELECT
        start_time,
        -- start_date,
        extract(hour from start_date) as hour,
        extract(day from start_date) as day,
        extract(week from start_date) as week,
        extract(month from start_date) as month,
        extract(year from start_date) as year,
        extract(dayofweek from start_date) as weekday
    FROM
        fact_songplays;
    """)

    create_db = ("""
        DROP TABLE IF EXISTS stage_events;
        DROP TABLE IF EXISTS stage_songs;
        DROP TABLE IF EXISTS fact_songplays;
        DROP TABLE IF EXISTS dim_user;
        DROP TABLE IF EXISTS dim_song;
        DROP TABLE IF EXISTS dim_artist;
        DROP TABLE IF EXISTS dim_time;

        CREATE TABLE stage_events
        (
            artist varchar(255)
            ,auth varchar(60)
            ,firstname varchar(60)
            ,gender varchar(30)
            ,iteminsession integer
            ,lastname varchar(60)
            ,length numeric(9,5)
            ,level varchar(4)
            ,location varchar(60)
            ,method varchar(10)
            ,page varchar(60)
            ,registration bigint
            ,sessionid integer
            ,song varchar(255)
            ,status smallint
            ,ts bigint
            ,useragent varchar(max)
            ,userid integer
        );

        COMMENT ON TABLE stage_events IS 'Stage table to load song play events from s3';

        CREATE TABLE stage_songs
        (
            num_songs integer
            ,artist_id varchar(30)
            ,artist_latitude numeric(9,6)  -- ex: 35.14968
            ,artist_longitude numeric(9,6) -- ex: -90.04892
            ,artist_location varchar(255)
            ,artist_name varchar(255)
            ,song_id varchar(30)
            ,title varchar(255)
            ,duration numeric(9,5)
            ,year smallint
        );

        COMMENT ON TABLE stage_songs IS 'Stage table to load song data from s3';


        CREATE TABLE IF NOT EXISTS dim_user
        (
            user_id integer
            ,first_name varchar(60)
            ,last_name varchar(60)
            ,gender varchar(1)
            ,level varchar(4)
            ,PRIMARY KEY (user_id)
        );

        COMMENT ON TABLE dim_user IS 'users in the app';
        COMMENT ON COLUMN dim_user.level IS 'paid or free';




        CREATE TABLE IF NOT EXISTS dim_song
        (
            song_id varchar(30)
            ,title varchar(255)
            ,artist_id varchar(30)
            ,year smallint
            ,duration numeric(10,6)
            ,PRIMARY KEY (song_id)
        );

        COMMENT ON TABLE dim_song IS 'songs in music database';


        CREATE TABLE IF NOT EXISTS dim_artist
        (
            artist_id varchar(30)
            ,name varchar(255)
            ,location varchar(255)
            ,latitude numeric(9,6)
            ,longitude numeric(9,6)
            ,PRIMARY KEY (artist_id)
        );

        COMMENT ON TABLE dim_artist IS 'artists in music database';

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

        CREATE TABLE IF NOT EXISTS fact_songplays
        (
            songplay_id bigint IDENTITY(0,1) NOT NULL
            ,start_time bigint      REFERENCES dim_time (start_time) NOT NULL
            ,user_id integer        REFERENCES dim_user (user_id)
            ,level varchar(4)       -- paid or free
            ,song_id varchar(30)    NULL REFERENCES dim_song (song_id)
            ,artist_id varchar(30)  NULL REFERENCES dim_artist (artist_id)
            ,session_id integer
            ,location varchar(255)
            ,user_agent varchar(400)
            ,start_date timestamp NOT NULL
            ,PRIMARY KEY (songplay_id)
        )
        DISTSTYLE KEY
        DISTKEY (song_id)
        COMPOUND SORTKEY (start_time, song_id, user_id, session_id)
        ;

        COMMENT ON TABLE fact_songplays IS 'records in event data associated with song plays i.e. records with page NextSong';
    """)
