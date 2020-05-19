import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType, FloatType
import pyspark.sql.functions as F
import boto3
import botocore

config = configparser.ConfigParser()
config.read('dl.cfg')
aws_region = config.get("AWS", 'REGION')
aws_access_key_id = config.get("AWS", 'ACCESS_KEY_ID')
aws_access_key_secret = config.get("AWS", 'SECRET_ACCESS_KEY')
aws_access_input_bucket = config.get("AWS", 'INPUT_BUCKET')
aws_access_output_bucket = config.get("AWS", 'OUTPUT_BUCKET')

s3 = boto3.resource(
    "s3", region_name=aws_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_access_key_secret
)

def create_spark_session():
    """
        This function creates the Spark Session that is used for processing the Data Lake
    """
    spark = SparkSession \
        .builder \
        .appName("udacity_dend_p4") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .config("mapreduce.input.fileinputformat.input.dir.recursive", True) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.com.amazonaws.services.s3.enableV4", "true") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_access_key_secret) \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100000") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function takes in the input_data path to obtain the source files (S3 or local).
        Performs ETL to generate the songs and artists dimension tables
    """

    # get filepath to song data file
    if input_data.startswith("s3a://"):
        # get objects to process from S3
        song_data = [
           f"{input_data}/{song_object.key}" for song_object in s3.Bucket(aws_access_input_bucket).objects.filter(Prefix="song_data") if song_object.key.endswith(".json")
        ]
    else:
        # get objects to process from local     
        song_data = f"{input_data}/song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        df.song_id, df.title, df.artist_id, df.year, df.duration        
    ).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print(f"[INFO] persisting for {songs_table.count()} songs")
    print("[INFO] songs_table schema:")
    songs_table.printSchema()    

    songs_table.write.parquet(
        f"{output_data}/songs", 
        partitionBy=['year', 'artist_id'],
        mode='overwrite'
    )

    # extract columns to create artists table
    artists_table = df.select(
        df.artist_id,
        df.artist_name.alias("name"), 
        df.artist_location.alias("location"), 
        df.artist_latitude.alias("latitude"), 
        df.artist_longitude.alias("longitude")
    ).dropDuplicates()
    
    # write artists table to parquet files
    print(f"[INFO] persisting for {artists_table.count()} artists")
    print("[INFO] artists_table schema:")
    artists_table.printSchema()    

    artists_table.write.parquet(
        f"{output_data}/artists",
        mode='overwrite'
    )    

def process_log_data(spark, input_data, output_data):
    """
        This function takes in the input_data path to obtain the source files (S3 or local).
        Performs ETL to generate the users, times dimension tables and songplays fact table
    """

    if input_data.startswith("s3a://"):
        # get objects to process from S3
        log_data = [
           f"{input_data}/{log_object.key}" for log_object in s3.Bucket(aws_access_input_bucket).objects.filter(Prefix="log_data") if log_object.key.endswith(".json")
        ]
    else:
        # get objects to process from local
        log_data = f"{input_data}/log-data/*.json" 

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.sort('ts', ascending=False).select(
        df.userId.alias('user_id'),
        df.firstName.alias('first_name'),
        df.lastName.alias('last_name'),
        df.gender,
        df.level
    ).dropDuplicates(['user_id', 'first_name', 'last_name', 'gender'])
    
    # write users table to parquet files
    print(f"[INFO] persisting for {users_table.count()} users")
    print("[INFO] users_table schema:")
    users_table.printSchema()    

    users_table.write.parquet(f"{output_data}/users", mode='overwrite')

    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda epoch: datetime.fromtimestamp(epoch / 1000.0), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        df.start_time.alias('start_time'),
        F.hour(df.start_time).alias('hour'),
        F.dayofmonth(df.start_time).alias('day'),
        F.weekofyear(df.start_time).alias('week'),
        F.month(df.start_time).alias('month'),
        F.year(df.start_time).alias('year'),
        F.date_format(df.start_time, 'E').alias('weekday')
    )
    
    # write time table to parquet files partitioned by year and month
    print(f"[INFO] persisting for {time_table.count()} times")
    print("[INFO] time_table schema:")
    time_table.printSchema()

    time_table.write.parquet(
            f"{output_data}/times",
            partitionBy=['year', 'month'],
            mode='overwrite'
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}/songs")
    artist_df = spark.read.parquet(f"{output_data}/artists")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(
        artist_df, df.artist == artist_df.name, "inner"
    ).join(
        song_df, [
            artist_df.artist_id == song_df.artist_id,
            df.song == song_df.title,
            df.length == song_df.duration
        ], "inner"
    ).select(
        df.start_time,
        df.userId.alias("user_id"),
        df.level,
        song_df.song_id,
        artist_df.artist_id,
        df.sessionId.alias("session_id"),
        df.location,
        df.userAgent.alias("user_agent"),
        F.year(df.start_time).alias('year'),
        F.month(df.start_time).alias('month')
    )

    # write songplays table to parquet files partitioned by year and month
    print(f"[INFO] persisting for {songplays_table.count()} songplays")
    print("[INFO] songplays_table schema:")
    songplays_table.printSchema()

    songplays_table.write.parquet(
            f"{output_data}/songplays",
            partitionBy=['year', 'month'],
            mode='overwrite'
    )    

def main():
    """
        Constructs the spark session, executes the ETL methods and stops the spark session
    """
    spark = create_spark_session()
    ## Local block
    input_data = "data/input"
    output_data = "data/output"

    ## S3 block
    # try:
    #     s3.meta.client.head_bucket(Bucket=aws_access_output_bucket)
    # except botocore.exceptions.ClientError as e:
    #     # If a client error is thrown, then check that it was a 404 error.
    #     # If it was a 404 error, then the bucket does not exist.
    #     error_code = e.response['Error']['Code']
    #     if error_code == '404':
    #         s3.create_bucket(Bucket=aws_access_output_bucket, CreateBucketConfiguration={'LocationConstraint': aws_region})
    #     else:
    #        raise    
    # input_data = f"s3a://{aws_access_input_bucket}"
    # output_data = f"s3a://{aws_access_output_bucket}/dend-p4"
 
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
