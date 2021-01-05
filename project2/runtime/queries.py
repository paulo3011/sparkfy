"""Utility queries."""

# sessionid is partition key and itemInSession is cluster key meaning the order of the heard song
artist_query = "select artist, song, length from music_history where session_id = 338 and item_in_session = 4"
artist_create_table = """
CREATE TABLE IF NOT EXISTS music_history (
    artist text,
    song text,
    length decimal,
    session_id int,
    item_in_session int,
    PRIMARY KEY (session_id,item_in_session)
)
"""