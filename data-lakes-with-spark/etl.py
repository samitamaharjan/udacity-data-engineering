import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a spark session.
    
    Returns:
        SparkSession object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function to process the song-data and create the songs table and the artists table from it.
    
    Parameters:
        spark: SparkSession object
        input_data (String): input file path (S3)
        output_data (String): output file path (local)
    """
    
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create view to write SQL
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                                SELECT song_id,
                                    title, 
                                    artist_id, 
                                    year, 
                                    duration
                                FROM song_data
                                WHERE song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT artist_id,
                                    artist_name as name,
                                    artist_location as location, 
                                    artist_latitude as latitude,  
                                    artist_longitude as longitude
                                FROM song_data
                                WHERE artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    This function is to process the event log data, then extract and transform the data into the desire format and load them into the tables - time, users and songplays.
    
    Parameters:
        spark: SparkSession object
        input_data (String): input file path in S3
        output_data (String): output file path in local
    """
    
    # get filepath to log data file
    log_data = input_data + 'log-data/*.json'

    # read log data file
    # mode='PERMISSIVE' would handle the corrupt data by setting null value if there is a corrupt data and put that malformed string
    # into the new column - corrupt_record. 
    df = spark.read \
                .json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record') \
                .drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table. drop_duplicated() is equivalent to DISTINCT in SQL queries.   
    users_table = df.select('userId', 'firstName' , 'lastName' , 'gender', 'level').drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users_table/'), mode='overwrite')

    # create timestamp column from original timestamp column.
    # create UDF to convert first milisecond time to seconds, then convert time in integer to String using utcfromtimestamp() 
    # and cast it as TimestampType. Here utcfromtimestamp will give us utc time. No need to convert the timezone.
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(x/1000), TimestampType())
    
    # implement UDF for ts field
    df = df.withColumn("start_time", get_timestamp("ts")) 
    
    # extract start_time column to create time table. drop_duplicated() is equivalent to DISTINCT in SQL queries.
    time_table = df.withColumn("hour", hour("start_time")) \
                    .withColumn("day", dayofmonth("start_time")) \
                    .withColumn("week", weekofyear("start_time")) \
                    .withColumn("month", month("start_time")) \
                    .withColumn("year", year("start_time")) \
                    .withColumn("weekday", dayofweek("start_time")) \
                    .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday") \
                    .drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time_table/'), mode='overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table/")
    
    # add songplay_id column to the existing df (log dataset). monotonically_increasing_id() is used to generate IDs with monotonically increasing and unique number.
    df = df.withColumn("songplay_id", monotonically_increasing_id())
   
    # extract columns from joined song and log datasets to create songplays table
    # Since we are planning to partition the output files by year and month, we added two more columns - year and month
    songplays_table = df.join(song_df, df.song == song_df.title) \
                        .select("songplay_id", 
                                df.start_time, 
                                col("userId").alias("user_id"), 
                                "level", 
                                "song_id", 
                                "artist_id", 
                                col("sessionId").alias("session_id"), 
                                "location", 
                                col("userAgent").alias("user_agent"), 
                                year("start_time").alias("year"), 
                                month("start_time").alias("month")
                                ) \
                        .drop_duplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays_table/"), mode="overwrite", partitionBy=("year", "month"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
