import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import IntegerType, TimestampType, lit, udf

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


@udf(TimestampType())
def get_timestamp(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000)


@udf(IntegerType())
def get_date_part(date, key):
    parts = {
        "hour": date.hour,
        "day": date.day,
        "week": date.isocalendar()[1],
        "month": date.month,
        "year": date.year,
        "weekday": date.isoweekday()
    }
    return parts[key]


def process_song_data(spark, input_data, output_data):
    df = spark.read.json(f"{input_data}/song-data/A/A/A/")

    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(f"{output_data}/songs.parquet")

    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    )
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artists.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}/log-data/"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    df.createOrReplaceTempView("logs_nextsong")

    # extract columns for users table
    users_table = spark.sql("""
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM next_songs
    """)

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}/users.parquet")

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", get_timestamp("ts"))

    date_parts = ["hour", "day", "week", "month", "year", "weekday"]
    for part in date_parts:
        df = df.withColumn(part, get_date_part("timestamp", lit(part)))

    # extract columns to create time table
    time_table = df.select("ts", *date_parts)

    # # write time table to parquet files partitioned by year and month
    time_table.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(f"{output_data}/time.parquet")

    # # read in song data to use for songplays table
    # song_df =

    # # extract columns from joined song and log datasets to create songplays table
    # songplays_table =

    # # write songplays table to parquet files partitioned by year and month
    # songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
