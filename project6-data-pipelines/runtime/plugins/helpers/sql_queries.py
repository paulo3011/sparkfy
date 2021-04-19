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

    total_play_quality_check = ("""
    SELECT
        count(0) AS total_plays
    FROM
        (
        SELECT
            DISTINCT ts, userid, sessionid AS start_time
        FROM
            stage_events
        WHERE
            page = 'NextSong')
    INTERSECT
    SELECT
        COUNT(0) AS total_plays
    FROM
        fact_songplays fsp
    JOIN dim_time ON
        dim_time.start_time = fsp.start_time
    WHERE
        dim_time.month = {{execution_date.strftime("%m")}}
        AND dim_time.year = {{execution_date.strftime("%Y")}}
    ;
    """, "> 0", "> 0")
