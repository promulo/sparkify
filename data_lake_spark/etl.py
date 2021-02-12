import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofmonth, hour, month, udf, weekofyear, year

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


def process_song_data(spark, input_data, output_data):
    df = spark.read.json(f"{input_data}/song-data/A/A/A/")

    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    songs_table.write\
        .partitionBy("year", "artist_id")\
        .mode("overwrite")\
        .parquet(f"{output_data}/songs.parquet")

    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    )
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artists.parquet")


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data =

#     # read log data file
#     df =

#     # filter by actions for song plays
#     df =

#     # extract columns for users table
#     artists_table =

#     # write users table to parquet files
#     artists_table

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df =

#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df =

#     # extract columns to create time table
#     time_table =

#     # write time table to parquet files partitioned by year and month
#     time_table

#     # read in song data to use for songplays table
#     song_df =

#     # extract columns from joined song and log datasets to create songplays table
#     songplays_table =

#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
