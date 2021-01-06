import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
""")

staging_songs_table_create = ("""
""")

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    id SERIAL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar NOT NULL,
    song_id varchar,
    artist_id varchar,
    session_id int NOT NULL,
    location varchar NOT NULL,
    user_agent text NOT NULL
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    id int PRIMARY KEY,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar(1) NOT NULL,
    level varchar NOT NULL
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int NOT NULL,
    duration float NOT NULL
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar NOT NULL,
    latitude float,
    longitude float
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    timestamp bigint PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)
"""

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
"""

user_table_insert = """
INSERT INTO users (id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET level = EXCLUDED.level
"""

song_table_insert = """
INSERT INTO songs (id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
"""

artist_table_insert = """
INSERT INTO artists (id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
"""

time_table_insert = """
INSERT INTO time (timestamp, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (timestamp) DO NOTHING
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
