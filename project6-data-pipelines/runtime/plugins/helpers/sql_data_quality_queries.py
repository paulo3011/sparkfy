class SqlDataQualityQueries:
    total_play_quality_check = ("""
    -- Ensure all unique records (events) in stage_events were inserted to fact table
    -- If a record is returned and the value is greater than zero, it means that the total number of records in the stage_events table is the same as that entered in the fact_songplays table
    SELECT
        count(0) AS total_plays
    FROM
        (
        SELECT
            DISTINCT ts, userid, sessionid
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
        fsp.start_time IN (SELECT start_time FROM dim_time
        WHERE
            dim_time.month = {{execution_date.strftime("%m")}}
            AND dim_time.year = {{execution_date.strftime("%Y")}})
    ;
    """, "== 1", "> 0")

    integrity_time_play_check = ("""
    -- Checks if there is a user with duplicate song play records on the same date and time
    -- The result needs to be zero or none
    SELECT COUNT(0) AS total
    FROM
        fact_songplays fsp
    JOIN dim_time ON dim_time.start_time = fsp.start_time
    WHERE
        dim_time.month = {{execution_date.strftime("%m")}}
        AND dim_time.year = {{execution_date.strftime("%Y")}}
    GROUP BY fsp.start_time, fsp.user_id HAVING COUNT(0) > 1
    LIMIT 1;
    """, "== 0", "")

    total_song_duration_check = ("""
    -- Ensure that total duration of songs is correct, dim_song have the same total duration as stage_songs
    -- The result expected is: returns a record with equal total duration
    select * from (
    select
    (select total_duration_dim_song from (select sum(duration) as total_duration_dim_song from dim_song)) as total_duration_dim_song
    ,(select total_duration_stage_songs from (select sum(duration) as total_duration_stage_songs from stage_songs)) as total_duration_stage_songs
    ) as d where d.total_duration_dim_song = d.total_duration_stage_songs
    ;
    """, "== 1", "")

    song_load_check = ("""
    -- Make sure all unique song on stage_songs table were imported
    -- Result expected: none song out of dim_song table
    SELECT * FROM (
        SELECT song_id, title, duration, "year", artist_id
        FROM stage_songs
        EXCEPT
        SELECT song_id, title, duration, "year", artist_id FROM dim_song
    ) LIMIT 1;
    """, "== 0", "")

    unique_song_check = ("""
    -- Make sure all song is unique on dim_song table
    -- Result expected: none song is duplicate on dim_song table
    SELECT song_id, COUNT(0) AS total_duplicated FROM dim_song group by song_id HAVING COUNT(0) > 1 LIMIT 1
    ;
    """, "== 0", "")

    user_load_check = ("""
    -- Make sure all unique user on stage_events table were imported
    -- Result expected: none user out of dim_user table
    SELECT * FROM (
        SELECT userid as user_id, firstname as first_name, lastname as last_name, gender, level
        FROM stage_events WHERE page='NextSong'
        EXCEPT
        SELECT user_id, first_name, last_name, gender, level FROM dim_user
    ) LIMIT 1;
    """, "== 0", "")

    unique_users_check = ("""
    -- Make sure all users is unique on dim_user table
    -- Result expected: none user is duplicate on dim_user table
    SELECT user_id, COUNT(0) AS total_duplicated FROM dim_user group by user_id HAVING COUNT(0) > 1 LIMIT 1
    ;
    """, "== 0", "")

    artist_load_check = ("""
    -- Make sure all unique artist on stage_songs table were imported
    -- Result expected: none artist out of dim_artist table
    SELECT * FROM (
        SELECT artist_id, artist_name AS "name", artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM stage_songs
        EXCEPT
        SELECT artist_id, "name", location, latitude, longitude FROM dim_artist
    ) LIMIT 1;
    """, "== 0", "")

    unique_artist_check = ("""
    -- Make sure all artist is unique on dim_artist table
    -- Result expected: none artist is duplicate on dim_artist table
    SELECT artist_id, COUNT(0) AS total_duplicated FROM dim_artist group by artist_id HAVING COUNT(0) > 1 LIMIT 1
    ;
    """, "== 0", "")

    time_load_check = ("""
    -- Make sure all unique music playback start time on stage_events table were imported
    -- Result expected: none music playback start time out of dim_time table
    SELECT * FROM (
		SELECT ts as start_time FROM stage_events WHERE page='NextSong'
	    EXCEPT
	    SELECT start_time FROM dim_time
   	) LIMIT 1;
    """, "== 0", "")

    unique_time_check = ("""
    -- Make sure all time is unique on dim_time table
    -- Result expected: none time is duplicate on dim_time table
    SELECT start_time, COUNT(0) AS total_duplicated FROM dim_time group by start_time HAVING COUNT(0) > 1 LIMIT 1
    ;
    """, "== 0", "")
