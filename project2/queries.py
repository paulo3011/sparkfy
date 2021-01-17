"""Project 2 queries."""

# TO-DO: Query 1:  Give me the artist, song title and song's length in
# the music app history that was heard during \
# sessionId = 338, and itemInSession = 4
# https://docs.datastax.com/en/dse/6.7/cql/cql/cql_reference/cql_commands/cqlSelect.html
# DataStax recommends limiting queries to a single partition using
# the WHERE clause. Queries across multiple partitions can impact performance.

# sessionid is partition key and itemInSession is cluster key meaning
# the order of the heard song
query1 = """
select artist, song, length
from music_history
where session_id = 338
and item_in_session = 4
"""

# All columns used in where clause needs to be in primary key
# (partition or clustering keys)
# seealso:
# https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/whereClustering.html
create_table1 = """
CREATE TABLE IF NOT EXISTS music_history (
    session_id int,
    item_in_session int,
    artist text,
    song text,
    length decimal,
    PRIMARY KEY (session_id,item_in_session)
)
"""

# __The sequence of the columns in CREATE statement and INSERT should be the
# same as to the order of COMPOSITE PRIMARY KEY and CLUSTERING COLUMNs.__
# (Udacity)
insert1 = """
insert into music_history (session_id, item_in_session, artist, song, length)
VALUES (%s,%s,%s,%s,%s)
"""

drop_table1 = "DROP TABLE IF EXISTS music_history"

# TO-DO: Query 2: Give me only the following: name of artist, song
# (sorted by itemInSession) and user (first and last name)\
# for userid = 10, sessionid = 182

query2 = """
select artist, song from artist_history
where user_id = 10 and session_id = 182
order by item_in_session, first_name, last_name"""

create_table2 = """
CREATE TABLE IF NOT EXISTS artist_history (
    user_id int,
    session_id int,
    item_in_session int,
    first_name text,
    last_name text,
    artist text,
    song text,
    PRIMARY KEY ((user_id,session_id), item_in_session)
)
"""

insert2 = """
insert into artist_history
(user_id, session_id, artist, song, item_in_session, first_name, last_name)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""

drop_table2 = "DROP TABLE IF EXISTS artist_history"

# TO-DO: Query 3: Give me every user name (first and last) in my music app
# history who listened to the song 'All Hands Against His Own'

query3 = """
select first_name, last_name
from user_history
where song='All Hands Against His Own'
"""

create_table3 = """
CREATE TABLE IF NOT EXISTS user_history (
    song text,
    user_id,
    first_name text,
    last_name text,
    PRIMARY KEY ((song, user_id), first_name, last_name)
)
"""

insert3 = """
insert into user_history (first_name, last_name, song)
VALUES (%s,%s,%s)
"""

drop_table3 = "DROP TABLE IF EXISTS user_history"

# utils

file = 'event_datafile_new.csv'

# column positions on csv file
artist = 0
first_name = 1
gender = 2
item_in_session = 3
last_name = 4
length = 5
level = 6
location = 7
session_id = 8
song = 9
user_id = 10
