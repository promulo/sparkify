import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import IntegerType, TimestampType, col, lit, month, udf, year

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
    df = spark.read.json(f"{input_data}/song_data/*/*/*")
    df.createOrReplaceTempView("songs_data")

    songs_table = spark.sql("""
        SELECT DISTINCT song_id AS id, title, artist_id, year, duration
        FROM songs_data
    """)
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(f"{output_data}/songs.parquet")

    artists_table = spark.sql("""
        SELECT DISTINCT artist_id AS id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM songs_data
    """)
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artists.parquet")


def process_log_data(spark, input_data, output_data):
    log_data = f"{input_data}/log_data/*/*"

    df = spark.read.json(log_data)

    df = df.filter(df.page == "NextSong")
    df.createOrReplaceTempView("logs_nextsong")

    users_table = spark.sql("""
        SELECT DISTINCT userId AS id, firstName AS first_name, lastName AS last_name, gender, level
        FROM logs_nextsong
    """)
    users_table.write.mode("overwrite").parquet(f"{output_data}/users.parquet")

    df = df.withColumn("timestamp", get_timestamp("ts"))

    date_parts = ["hour", "day", "week", "month", "year", "weekday"]
    for part in date_parts:
        df = df.withColumn(part, get_date_part("timestamp", lit(part)))

    time_table = df.select("ts", *date_parts)
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(f"{output_data}/time.parquet")

    df.createOrReplaceTempView("logs_nextsong")

    songs_df = spark.read.parquet(f"{output_data}/songs.parquet")
    songs_df.createOrReplaceTempView("songs")

    artists_df = spark.read.parquet(f"{output_data}/artists.parquet")
    artists_df.createOrReplaceTempView("artists")

    songplays_table = spark.sql("""
        SELECT timestamp AS start_time, userId AS user_id, level, s.id AS song_id, a.id AS artist_id, sessionId AS session_id, l.location, userAgent AS user_agent
        FROM logs_nextsong l
        JOIN songs s ON s.title = l.song
        JOIN artists a ON a.name = l.artist
    """)
    songplays_table \
        .withColumn("year", year(col("start_time"))) \
        .withColumn("month", month(col("start_time"))) \
        .write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(f"{output_data}/songplays.parquet")


def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-paulo/sparkify/tables"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
