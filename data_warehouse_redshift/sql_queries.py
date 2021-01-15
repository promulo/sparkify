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

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    id BIGINT IDENTITY(0,1) PRIMARY KEY,
    artist TEXT,
    auth TEXT,
    first_name TEXT,
    gender VARCHAR(1),
    item_in_session INT,
    last_name TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration BIGINT,
    session_id INT,
    song TEXT,
    status INT,
    ts BIGINT,
    user_agent TEXT,
    user_id TEXT
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_location TEXT,
    artist_longitude FLOAT,
    artist_name TEXT,
    duration FLOAT,
    num_songs INT,
    song_id TEXT,
    title TEXT,
    year INT
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id TEXT NOT NULL,
    level TEXT NOT NULL,
    song_id TEXT DISTKEY,
    artist_id TEXT,
    session_id INT NOT NULL,
    location TEXT NOT NULL,
    user_agent TEXT NOT NULL
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender VARCHAR(1) NOT NULL,
    level TEXT NOT NULL
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    latitude FLOAT,
    longitude FLOAT
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    timestamp BIGINT PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
"""

# STAGING TABLES

staging_events_copy = f"""
COPY staging_events FROM '{config.get("S3", "LOG_DATA")}'
CREDENTIALS 'aws_iam_role={config.get("IAM_ROLE", "ARN")}'
COMPUPDATE OFF REGION 'us-west-2'
JSON '{config.get("S3", "LOG_JSONPATH")}'
"""

staging_songs_copy = f"""
COPY staging_songs FROM '{config.get("S3", "SONG_DATA")}'
CREDENTIALS 'aws_iam_role={config.get("IAM_ROLE", "ARN")}'
COMPUPDATE OFF REGION 'us-west-2'
JSON 'auto'
"""

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + (ts / 1000) * interval '1 second', user_id, level, song_id, artist_id, session_id, location, user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.artist = ss.artist_name AND se.page = 'NextSong'
"""

user_table_insert = """
INSERT INTO users (id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong' AND user_id NOT IN (SELECT id FROM users)
"""

song_table_insert = """
INSERT INTO songs (id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT id FROM songs)
"""

artist_table_insert = """
INSERT INTO artists (id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT id FROM artists)
"""

time_table_insert = """
INSERT INTO time (timestamp, hour, day, week, month, year, weekday)
    WITH parsed_ts AS (
        SELECT ts, timestamp 'epoch' + (ts / 1000) * interval '1 second' AS parsed
        FROM staging_events
        WHERE page = 'NextSong'
    )
    SELECT ts,
           EXTRACT(HOUR FROM parsed),
           EXTRACT(DAY FROM parsed),
           EXTRACT(WEEK FROM parsed),
           EXTRACT(MONTH FROM parsed),
           EXTRACT(YEAR FROM parsed),
           EXTRACT(WEEKDAY FROM parsed)
    FROM parsed_ts
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
