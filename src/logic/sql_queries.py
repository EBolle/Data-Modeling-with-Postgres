from psycopg2 import sql


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT exists songplays (
    songplay_id serial PRIMARY KEY,
    start_time timestamp,
    user_id int,
    level text,
    song_id text,
    artist_id text, 
    session_id int,
    location text,
    user_agent text
);
"""

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

songs_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY,   
    title text,
    artist_id text,
    year int,
    duration real,
    CONSTRAINT not_null_check CHECK (NOT (title, artist_id, year, duration) IS NULL)
);
"""

artists_table_create = """
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

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO UPDATE SET gender = EXCLUDED.gender, level = EXCLUDED.level; 
"""

songs_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING; 
"""

artists_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING; 
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) 
DO NOTHING; 
"""

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, songs_table_create, artists_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, songs_table_drop, artists_table_drop, time_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, songs_table_insert, artists_table_insert, time_table_insert]