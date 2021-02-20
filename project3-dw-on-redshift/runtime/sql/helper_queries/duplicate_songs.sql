-- CHECK DUPLICATE ROWS
-- 0 duplicate
SELECT
	song_id
	,COUNT(0)
FROM stage_songs
GROUP BY song_id HAVING COUNT(song_id) > 1;