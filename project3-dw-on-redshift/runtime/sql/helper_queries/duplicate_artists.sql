-- updated rows 10025
DROP TABLE IF EXISTS tmp_duplicated_songs;
CREATE TEMPORARY TABLE tmp_duplicated_songs
AS
SELECT
    artist_id
    ,artist_name
    ,artist_location
    ,artist_latitude
    ,artist_longitude
    ,ROW_NUMBER () OVER (
    	PARTITION BY artist_id
    	ORDER BY (has_artist_name + has_artist_location + has_artist_latitude + has_artist_longitude) DESC )
    	AS row_id
	,(has_artist_name + has_artist_location + has_artist_latitude + has_artist_longitude) AS rank
FROM (
	SELECT DISTINCT
	    artist_id
	    ,artist_name
	    ,artist_location
	    ,artist_latitude
	    ,artist_longitude
	    ,case when artist_name is not null then 1 else 0 end  	  AS has_artist_name
	    ,case when artist_location is not null then 1 else 0 end  AS has_artist_location
	    ,case when artist_latitude is not null then 1 else 0 end  AS has_artist_latitude
	    ,case when artist_longitude is not null then 1 else 0 end AS has_artist_longitude
	FROM tmpsongs
) as songs
;

SELECT * FROM tmp_duplicated_songs WHERE artist_id='AR5LMPY1187FB573FE';

/*
artist_id         |artist_name      |artist_location|artist_latitude|artist_longitude|row_id|rank|
------------------|-----------------|---------------|---------------|----------------|------|----|
AR5LMPY1187FB573FE|Chaka Khan_ Rufus|Chicago, IL    |      41.884149|      -87.632409|     1|   4|
AR5LMPY1187FB573FE|Chaka Khan_ Rufus|               |               |                |     2|   2|
*/

select * from dim_artist where artist_id='AR5LMPY1187FB573FE';