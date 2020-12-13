from psycopg2 import sql

# DROP TABLES

drop_all_tables = "DROP TABLE IF EXISTS songplays, users, songs, artists, time"

# CREATE TABLES

songplay_table_create = ("""
""")

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id text PRIMARY KEY,   
    first_name text,
    last_name text,
    gender text,
    level text,
    CONSTRAINT not_null_check CHECK (NOT (level) IS NULL)
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY,   
    title text,
    artist_id text,
    year int,
    duration real,
    CONSTRAINT not_null_check CHECK (NOT (title, artist_id, year, duration) IS NULL)
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY,   
    name text,
    location text,
    latitude real,
    longitude real,
    CONSTRAINT not_null_check CHECK (NOT (name) IS NULL)
);
"""

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,   
    hour int,
    day int,
    month int,
    year int,
    weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = sql.SQL("""INSERT INTO "songs" (song_id, title, artist_id, year, duration) VALUES %s""")

artist_table_insert = sql.SQL("""INSERT INTO "artists" (artist_id, name, location, latitude, longitude) VALUES %s""")

time_table_insert = ("""
""")

# song_table_create = """
# CREATE TABLE IF NOT EXISTS songs (
#     song_id serial PRIMARY KEY,
#     title text,
#     artist_id text,
#     year int,
#     duration real,
#     CONSTRAINT not_null_check CHECK (NOT (title, artist_id, year, duration) IS NULL)
# );
# """

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create]
insert_table_queries = [artist_table_insert, song_table_insert]