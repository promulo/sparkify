# Sparkify - Data Warehousing with Amazon Redshift

## Introduction

Sparkify is a startup in the segment of music streaming. They have a
very popular app which gives their users access to a large catalog
of songs from artists from all over the world. In order to better
drive business decisions, Sparkify recently started to collect all
sorts of user activity data which should fuel several planned data
analysis and machine learning pipelines.

This project focuses performing some data warehousing of song play activity
data based on raw datasets (JSON-based) gathered by Sparkify's backend.
That is done by an ETL pipeline written in Python. The DBMS of choice is
Amazon Redshift.

## The Model

The ETL process starts by loading the raw data files from S3 into
two Redshift staging tables. After the initial load is complete, the
script further processes the data by inserting it into five tables
which form the final data model. The model consists of one fact table
and four dimension tables. All tables are detailed in the next sections.

### Staging table: `staging_events`

This intermediate table consists of the initial load of raw event data files
stored in S3.

```sql
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
```

### Staging table: `staging_songs`

This table is used as staging storage for individual song data stored in S3.

```sql
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
```

### Fact table: `songplays`

This fact table holds data about songplay events from users of Sparkify's
music app.

```sql
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
```

### Dimension table: `songs`

This dimension table holds data about songs available on Sparkify's catalog.

```sql
CREATE TABLE IF NOT EXISTS songs (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL
)
```

### Dimension table: `artists`

This dimension table holds data about artists availble on Sparkify's catalog.

```sql
CREATE TABLE IF NOT EXISTS artists (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    latitude FLOAT,
    longitude FLOAT
)
```

### Dimension table: `users`

This dimension table holds data about users of Sparkify's music app.

```sql
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender VARCHAR(1) NOT NULL,
    level TEXT NOT NULL
)
```

### Dimension table: `time`

This dimension table keeps track of timestamp information from songplay events.

```sql
CREATE TABLE IF NOT EXISTS time (
    timestamp BIGINT PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
```

## Directory structure

This project consists of the files listed below.

```
data_warehouse_redshift/
├── README.md
├── create_tables.py
├── dwh.cfg
├── etl.py
├── requirements.txt
├── sql_queries.py
```
* `README.md`: This README file
* `create_tables.py`: Python script used for creating/resetting the database used by the ETL process
* `dwh.cfg`: Configuration file supporting AWS Redshift and S3 access
* `etl.py`: Python script consisting of the ETL pipeline
* `requirements.txt`: Runtime dependencies listing to be used by `pip` (PyPI)
* `sql_queries.py`: Python file containing all the SQL queries used by the other Python scripts

## ETL pipeline usage

The ETL pipeline processes the raw data stored in S3 and populates the abovementioned tables.
The raw data consist of several JSON-based files placed in the S3 bucket.

### Requirements

- Python 3.7 or above
- psycopg2
- AWS Redshift cluster up and running (see `dwh.cfg`)

It is recommended the use of Python's `virtualenv` module for ensuring runtime isolation.
In Debian-based systems, it might be necessary to install the additional
`python3-virtualenv` package (requires root access). System administration assistance
may be required if the target system is not Debian-based or the user has not enough
privileges to install packages.

### `etl.py` script usage

The ETL processing happens in the `etl.py` script. In order to make use of it, some
required steps are necessary beforehand. The following assumes the use of Python
virtual environments. It is also assumed that the user is using a Unix-like terminal
and is currently working from the `data_warehouse_redshift` directory.

1. Create a virtual environment

```
$ python3 -m venv .venv
$ source .venv/bin/activate
```

2. Install Python runtime dependencies

```
$ pip install -r requirements.txt
```

If a virtual environment is not used, the above command will require either root
access or the use of the `--user` flag.

3. Create/reset tables via the `create_tables.py` script

```
$ python create_tables.py
```

4. Run the ETL script and populate the tables

```
$ python etl.py
```

The script does not produce any output unless there is an error. The initial load can
take a long time to finish depending on the amount of data to be loaded (that is limited
in the configuration file in order to avoid query timeout). After it finishes execution,
the tables will be populated in the Redshift cluster.
