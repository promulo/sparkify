# Sparkify - Relational Data Model

## Introduction

Sparkify is a startup in the segment of music streaming. They have a
very popular app which gives their users access to a large catalog
of songs from artists from all over the world. In order to better
drive business decisions, Sparkify recently started to collect all
sorts of user activity data which should fuel several planned data
analysis and machine learning pipelines.

This project focuses on modelling and processing of song play activity
data based on raw datasets (JSON-based) gathered by Sparkify's backend.
That is done by an ETL pipeline written in Python. The DBMS of choice is
PostgreSQL 12, which means the data was modelled using a traditional
relational approach.

## The Model

The ETL script processes the raw data files and loads the processed data
into five tables which form the data model. The model consists of one
fact table and four dimension tables. All tables are detailed in the next
sections.

### Fact table: `songplays`

This fact table holds data about songplay events from users of Sparkify's
music app.

```sql
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
```

### Dimension table: `songs`

This dimension table holds data about songs available on Sparkify's catalog.

```sql
CREATE TABLE IF NOT EXISTS songs (
    id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int NOT NULL,
    duration float NOT NULL
)
```

### Dimension table: `artists`

This dimension table holds data about artists availble on Sparkify's catalog.

```sql
CREATE TABLE IF NOT EXISTS artists (
    id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar NOT NULL,
    latitude float,
    longitude float
)
```

### Dimension table: `users`

This dimension table holds data about users of Sparkify's music app.

```sql
CREATE TABLE IF NOT EXISTS users (
    id int PRIMARY KEY,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar(1) NOT NULL,
    level varchar NOT NULL
)
```

### Dimension table: `time`

This dimension table keeps track of timestamp information from songplay events.

```sql
CREATE TABLE IF NOT EXISTS time (
    timestamp bigint PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)
```

## Directory structure

This project consists of the files listed below.

```
data_modelling_pg/
├── README.md
├── create_tables.py
├── data
├── etl.ipynb
├── etl.py
├── requirements.txt
├── sql_queries.py
└── test.ipynb
```
* `README.md`: This README file
* `create_tables.py`: Python script used for creating/resetting the database used by the ETL process
* `data`: Directory containing the raw data JSON-based files
* `etl.ipynb`: Jupyter notebook used as a prototype for the ETL process
* `etl.py`: Python script consisting of the ETL pipeline
* `requirements.txt`: Runtime dependencies listing to be used by `pip` (PyPI)
* `sql_queries.py`: Python file containing all the SQL queries used by the other Python scripts
* `test.ipynb`: Jupyter notebook used for checking state of database tables during testing

## ETL pipeline usage

The ETL pipeline processes the raw data and populates the abovementioned tables.
The raw data consist of several JSON-based files placed in the `data` directory.
Two types of files are handled by the `etl.py` script: **log data** files containing
songplay events and **song data** files containing individual songs information.

### Requirements

- Python 3.7 or above
- PostgreSQL 12

It is recommended the use of Python's `virtualenv` module for ensuring runtime isolation.
In Debian-based systems, it might be necessary to install the additional
`python3-virtualenv` package (requires root access). System administration assistance
may be required if the target system is not Debian-based or the user has not enough
privileges to install packages.

### `etl.py` script usage

The ETL processing happens in the `etl.py` script. In order to make use of it, some
required steps are necessary beforehand. The following assumes the use of Python
virtual environments. It is also assumed that the user is using a Unix-like terminal 
and is currently working from the `data_modelling_pg` directory.

1. Create a virtual environment

```
$ python3 -m venv venv
$ source ./venv/bin/activate
```

2. Install Python runtime dependencies

```
$ pip install -r requirements.txt
```

If a virtual environment is not used, the above command will require either root
access or the use of the `--user` flag.

3. Create `sparkifydb` database and tables via the `create_tables.py` script

```
$ python create_tables.py
```

4. Run the ETL script and populate the tables

```
$ python etl.py
```

The script outputs the amount of processed files. After it finishes execution, the
tables will be populated.
