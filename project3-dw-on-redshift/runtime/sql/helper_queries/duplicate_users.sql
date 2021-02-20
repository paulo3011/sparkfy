select
	userid
	,firstname
	,lastname
	,gender
	,count(userid) as total
from
(
	SELECT DISTINCT
		userid
		,firstname
		,lastname
		,gender
		,level
	FROM stage_events
) as users
group by
	userid
	,firstname
	,lastname
	,gender
having count(userid) > 1
;

SELECT DISTINCT
	userid
	,firstname
	,lastname
	,gender
	,level
FROM stage_events
where userid in (49);

/*
userid|firstname|lastname|gender|level|
------|---------|--------|------|-----|
    49|Chloe    |Cuevas  |F     |paid |
    49|Chloe    |Cuevas  |F     |free |
*/