import os
from pathlib import Path

import pandas as pd
import psycopg2

from create_tables import create_tables, drop_tables
from sql_queries import (artist_table_insert, song_select, song_table_insert,
                         songplay_table_insert, time_table_insert,
                         user_table_insert)


def process_song_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)

    song_data = df[
        [
            'song_id',
            'title',
            'artist_id',
            'year',
            'duration'
        ]
    ].values[0]
    cur.execute(song_table_insert, tuple(song_data))

    artist_data = df[
        [
            'artist_id',
            'artist_name',
            'artist_location',
            'artist_latitude',
            'artist_longitude'
        ]
    ].values[0]
    cur.execute(artist_table_insert, tuple(artist_data))


def process_log_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == 'NextSong']

    t = df['ts'].apply(pd.to_datetime, unit='ms')

    time_data = {
        'timestamp': list(df['ts']),
        'hour': list(t.dt.hour.values),
        'day': list(t.dt.day.values),
        'week': list(t.dt.isocalendar().week.values),
        'month': list(t.dt.month.values),
        'year': list(t.dt.year.values),
        'weekday': list(t.dt.weekday.values)
    }
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for row in time_df.itertuples():
        cur.execute(time_table_insert, row[1:])

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for row in user_df.itertuples():
        cur.execute(user_table_insert, row[1:])

    for row in df.itertuples():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            song_id, artist_id = results
        else:
            song_id = artist_id = None

        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    all_files = []
    for root, _, files in os.walk(filepath):
        files = Path(root).glob('*.json')
        for f in files :
            all_files.append(Path(f).resolve())

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user=student password=student')
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == '__main__':
    main()
