import configparser
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import IntegerType, LongType

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("aws", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("aws", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    return spark


@udf(IntegerType())
def get_date_part(date_delta, key):
    date = datetime(year=1960, month=1, day=1) + timedelta(days=int(date_delta))
    parts = {
        "day": date.day,
        "weekday": date.isoweekday(),
        "month": date.month,
        "year": date.year
    }
    return parts[key]


def process_i94_log_data(spark, input_data, output_data):
    df = spark \
        .read \
        .format("com.github.saurfang.sas.spark") \
        .load(f"{input_data}/i94_apr16_sub.sas7bdat")

    date_parts = ["day", "weekday", "month", "year"]
    for part in date_parts:
        df = df.withColumn(part, get_date_part("arrdate", lit(part)))

    df.createOrReplaceTempView("arrivals")

    time_table = spark.sql("""
        SELECT DISTINCT arrdate, day, weekday, month, year FROM arrivals
    """)
    time_table \
        .write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(f"{output_data}/time.parquet")

    people_table = df.select(
        df.admnum.cast(LongType()).alias("admission_number"),
        df.biryear.cast(IntegerType()).alias("birth_year"),
        df.gender,
        df.i94addr.alias("address_state")
    )
    people_table \
        .write \
        .partitionBy("gender", "birth_year") \
        .mode("overwrite") \
        .parquet(f"{output_data}/people.parquet")

    visas_table = spark.sql("""
        SELECT DISTINCT visatype AS type,
        CASE
            WHEN i94visa = 1.0 THEN 'business'
            WHEN i94visa = 2.0 THEN 'leisure'
            WHEN i94visa = 3.0 THEN 'student'
        END AS purpose
        FROM arrivals
    """)
    visas_table \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_data}/visas.parquet")

    arrivals_table = df.select(
        df.cicid.cast(LongType()).alias("id"),
        df.admnum.cast(LongType()).alias("person_id"),
        df.visatype.alias("visa"),
        df.i94res.cast(IntegerType()).alias("country_of_origin"),
        df.i94port.alias("port_of_entry"),
        df.arrdate.cast(LongType()).alias("date"),
        df.airline,
        df.fltno.alias("flight_number"),
        df.dtaddto.alias("allowed_until_date"),
        df.visapost.alias("visa_issuer")
    )
    arrivals_table \
        .write \
        .partitionBy("date") \
        .mode("overwrite") \
        .parquet(f"{output_data}/arrivals.parquet")


def process_ports_raw_data(spark, input_data, output_data):
    US_STATES = (
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
        "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
        "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
        "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    )

    df_ports = spark.read.csv(f"{input_data}/i94_ports_all.csv", header=True).dropna()
    df_ports.createOrReplaceTempView("ports_raw")

    ports_of_entry_table = spark.sql(f"""
        SELECT DISTINCT i94_port_code AS code, i94_port_name AS name, i94_port_state AS state
        FROM ports_raw
        WHERE i94_port_state IN {US_STATES}
    """)
    ports_of_entry_table \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_data}/ports_of_entry.parquet")


def process_countries_raw_data(spark, input_data, output_data):
    df = spark.read.csv(f"{input_data}/i94_countries_all.csv", header=True).dropna()

    df = df.where(
        ~(df.i94_country_name.startswith('No Country Code')
          | df.i94_country_name.startswith('Collapsed'))
    )
    df = df.withColumn("country_code_as_int", df.i94_country_code.cast(IntegerType()))

    remove_prefix_udf = udf(lambda value, prefix: value.replace(prefix, ''))
    df = df.withColumn(
        "country_name_without_prefix", remove_prefix_udf("i94_country_name", lit("INVALID: "))
    )

    countries_table = df.select(
        df.country_code_as_int.alias("i94_code"),
        df.country_name_without_prefix.alias("name")
    )
    countries_table \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_data}/countries.parquet")


def main():
    spark = create_spark_session()

    input_data = "s3a://paulo-dend/capstone/data"
    output_data = "s3a://paulo-dend/capstone/tables"

    process_countries_raw_data(spark, input_data, output_data)
    process_ports_raw_data(spark, input_data, output_data)
    process_i94_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
