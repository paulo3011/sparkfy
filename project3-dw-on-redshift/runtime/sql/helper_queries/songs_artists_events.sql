/*
ETL

1. stage_events => dim_time (GET UNIQUE TIMESTAMP)
2. stage_events => dim_user (GET THE LAST INFORMATION FROM THIS TABLE, E.G. level - 96 USERS)
3. stage_songs  => dim_song (GET UNIQUE SONG DATA - 14896 SONGS). Theres no duplicate row in stage_songs
    - OK: USE btrim ON NAMES,TITLE

4. stage_songs  => dim_artist (GET THE LAST INFORMATION FROM THIS TABLE). Here there are deferents artist name.
    - OK: USE btrim ON NAMES,TITLE

5. (stage_songs + stage_events) => fact_songplays
    5.1 (
        dim_song.title = BTRIM(stage_events.song)
        and dim_artist.name = BTRIM(stage_events.artist)
        and dim_song.duration = stage_events.length
    )
    5.2 (
        dim_song.title = BTRIM(stage_events.song)
        AND dim_artist.name = BTRIM(stage_events.artist)
        AND (stage_events.artist_id is null or stage_events.song_id is null)
    )
*/

-- song's name on event table with space problems

SELECT
	len(ev.song) as norma_size
	,len(btrim(ev.song)) as trim_size
	,btrim(ev.song) as song_without_spaces
	,ev.*
FROM
	stage_events ev
WHERE
	page='NextSong'
	AND len(ev.song) <> len(btrim(ev.song))
;

/*
norma_size|trim_size|song_without_spaces        |artist                        |auth     |firstname|gender|iteminsession|lastname |length   |level|location                               |method|page    |registration |sessionid|song                        |status|ts           |useragent                                                                                                                 |userid|event_date             |song_id|artist_id|
----------|---------|---------------------------|------------------------------|---------|---------|------|-------------|---------|---------|-----|---------------------------------------|------|--------|-------------|---------|----------------------------|------|-------------|--------------------------------------------------------------------------------------------------------------------------|------|-----------------------|-------|---------|
        15|       14|I Feel So Fine             |KMC Feat. Dhany               |Logged In|Mohammad |M     |           18|Rodriguez|483.81342|paid |Sacramento--Roseville--Arden-Arcade, CA|PUT   |NextSong|1540511766796|      969|I Feel So Fine              |   200|1543348532796|"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"|    88|2018-11-27 19:55:32.000|       |         |
        18|       17|Who's Your Daddy?          |Benny Benassi                 |Logged In|Tegan    |F     |            8|Levine   |207.49016|paid |Portland-South Portland, ME            |PUT   |NextSong|1540794356796|      409|Who's Your Daddy?           |   200|1541773832796|"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"|    80|2018-11-09 14:30:32.000|       |         |
        11|       10|Heavy Soul                 |The Black Keys                |Logged In|Tegan    |F     |           20|Levine   |156.18566|paid |Portland-South Portland, ME            |PUT   |NextSong|1540794356796|      992|Heavy Soul                  |   200|1543351560796|"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"|    80|2018-11-27 20:46:00.000|       |         |
        22|       21|Love Is Gonna Save Us      |Benny Benassi Presents The Biz|Logged In|Chloe    |F     |            7|Cuevas   |205.26974|paid |San Francisco-Oakland-Hayward, CA      |PUT   |NextSong|1540940782796|      630|Love Is Gonna Save Us       |   200|1542315214796|Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0                                                         |    49|2018-11-15 20:53:34.000|       |         |
        13|       12|Rikkalicious               |HervÃÂ© & Kissy Sellout      |Logged In|Chloe    |F     |           23|Cuevas   |341.41994|paid |San Francisco-Oakland-Hayward, CA      |PUT   |NextSong|1540940782796|      849|Rikkalicious                |   200|1543077522796|Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0                                                         |    49|2018-11-24 16:38:42.000|       |         |
        28|       27|I Will Not Reap Destruction|We Came As Romans             |Logged In|Jacob    |M     |           31|Klein    |238.13179|paid |Tampa-St. Petersburg-Clearwater, FL    |PUT   |NextSong|1540558108796|      692| I Will Not Reap Destruction|   200|1543069415796|"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"   |    73|2018-11-24 14:23:35.000|       |         |
*/

-- rows on stage_songs with space problems

SELECT
	len(s.title) as norma_size
	,len(btrim(s.title)) as trim_size
	,btrim(s.title) as song_without_spaces
	,s.*
FROM
	stage_songs s
WHERE
	len(s.title) <> len(btrim(s.title))
;

/*
norma_size|trim_size|song_without_spaces   |num_songs|artist_id         |artist_latitude|artist_longitude|artist_location|artist_name                             |song_id           |title                  |duration |year|
----------|---------|----------------------|---------|------------------|---------------|----------------|---------------|----------------------------------------|------------------|-----------------------|---------|----|
        19|       18|She Left Me A Mule    |        1|ARWEM1D1187FB4077B|      54.074849|       -2.719390|Caton, MS      |Sonny Landreth                          |SOOUQAZ12AB017BDB0|She Left Me A Mule     |212.63628|1999|
        16|       15|Dry Techterlech       |        1|AR2SZ2A1187FB5A5FF|               |                |               |Chava Alberstein                        |SOONYFK12A6D4F442C| Dry Techterlech       |231.83627|   0|
        22|       21|You Make Me Feel Good |        1|ARUIRXF1187FB45BC6|      31.168900|     -100.077150|Texas          |JK                                      |SORMUQC12A8C13C1D7|You Make Me Feel Good  |361.16852|2003|
        12|       11|Rep Yo Shit           |        1|ARAWH5O1187B9892A4|      38.997920|     -105.550960|Colorado       |P.C.P (Sick Jacken & Necro)             |SOMYKLK12AB0181F23|Rep Yo Shit            |182.64770|   0|
        23|       22|Death Frees Every Soul|        1|ARGVYYD11C8A416342|               |                |               |DJ Muggs & Planet Asia feat. Sick Jacken|SOTMGYY12A8C140E5D|Death Frees Every Soul |222.17097|2008|
        17|       16|The New Resident      |        1|ARLRRTC12420780AA2|               |                |               |MadlibThe Beat Konducta                 |SOZOIFI12A8C142C94|The New Resident       | 93.85751|   0|
        17|       16|Bad Meaning Good      |        1|ARTT3NL1187B9B5172|      37.271880|     -119.270230|Analog City, CA|Slakah the Beatchild Feat. Drake        |SOKZXNY12A8C1441EA|Bad Meaning Good       |161.61913|   0|
        17|       16|Every Single Day      |        1|ARKJGXB1187FB4D8C9|               |                |               |Benassi Bros. Feat. Dhany               |SODABWY12A58A7D5BB|Every Single Day       |214.67383|2005|
*/

-- sample events with song without relation to stage_songs

SELECT
	ev.*
FROM
	stage_events ev
LEFT JOIN stage_songs s on (s.title = ev.song)
WHERE
	page='NextSong'
	AND s.song_id is null
;


-- SAMPLE SONG NOT IN stage_songs data

select
	ev.*
from
	stage_events ev
left join
	dim_artist a on a."name" = ev.artist
where
	ev.artist like '%N.E.R.D.%';
;

/*
artist                   |auth     |firstname|gender|iteminsession|lastname|length   |level|location                          |method|page    |registration |sessionid|song                       |status|ts           |useragent                                                                                                                                |userid|event_date             |song_id|artist_id|
-------------------------|---------|---------|------|-------------|--------|---------|-----|----------------------------------|------|--------|-------------|---------|---------------------------|------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------|------|-----------------------|-------|---------|
N.E.R.D.                 |Logged In|Kate     |F     |           15|Harrell |259.23872|paid |Lansing-East Lansing, MI          |PUT   |NextSong|1540472624796|      633|Rock Star                  |   200|1542389929796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"                               |    97|2018-11-16 17:38:49.000|       |         |
N.E.R.D.                 |Logged In|Layla    |F     |           35|Griffin |259.23872|paid |Lake Havasu City-Kingman, AZ      |PUT   |NextSong|1541057188796|      672|Rock Star                  |   200|1542601925796|"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"                          |    24|2018-11-19 04:32:05.000|       |         |
N.E.R.D.                 |Logged In|James    |M     |            0|Martin  |242.99056|free |Dallas-Fort Worth-Arlington, TX   |PUT   |NextSong|1540810448796|       78|Provider (Remix Radio Edit)|   200|1542536291796|Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)                                                                          |    79|2018-11-18 10:18:11.000|       |         |
N.E.R.D.                 |Logged In|Lily     |F     |           11|Koch    |259.23872|paid |Chicago-Naperville-Elgin, IL-IN-WI|PUT   |NextSong|1541048010796|      834|Rock Star                  |   200|1543231001796|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|    15|2018-11-26 11:16:41.000|       |         |
N.E.R.D.                 |Logged In|Chloe    |F     |            5|Cuevas  |259.23872|paid |San Francisco-Oakland-Hayward, CA |PUT   |NextSong|1540940782796|      621|Rock Star                  |   200|1542300174796|Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0                                                                        |    49|2018-11-15 16:42:54.000|       |         |
N.E.R.D. FEATURING MALICE|Logged In|Jayden   |M     |            0|Fox     |288.99220|free |New Orleans-Metairie, LA          |PUT   |NextSong|1541033612796|      184|Am I High (Feat. Malice)   |   200|1541121934796|"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"                          |   101|2018-11-02 01:25:34.000|       |         |                                                                                                           |
*/


/*
event sample with artist diferent from song table
artist_name|song_id           |title                        |duration
-----------|------------------|-----------------------------|---------
N.E.R.D.   |SORCKJU12A67021859|Don't Worry About It (Edited)|221.54404
*/

-- 0 rows
SELECT * FROM dim_song where title like '%Am I High%';
-- 0 rows
SELECT * FROM stage_songs where title like '%Am I High%';

SELECT * FROM dim_artist where "name" like '%N.E.R.D%';
/*
artist_id         |name    |location|latitude|longitude|
------------------|--------|--------|--------|---------|
ARHAEXZ1187FB54F3C|N.E.R.D.|        |        |         |
*/