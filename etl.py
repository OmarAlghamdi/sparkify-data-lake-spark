import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, from_unixtime, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

# read configurations
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = config.get('AWS', 'BUCKET_NAME')


def create_spark_session():
    """
    Creates a new Spark session and retrun it.
    
    The Spark session is configured to connect to AWS S3
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read from S3 and process song data into songs table and artist table.
    
    Song data is read from a public S3 bucket, stored as json files. Then, songs
    dataframe and artist dataframe are extracted based on the relevent columns in each.
    Finally, the tables are stored back to a private S3 bucket in parquet fromat.
    Songs table is stored under '/songs' path and artists table is stored under '/artists' path.
    
    Parameters:
        spark: Spark session
        input_data: path to songs data in a public S3 bucket
        output_data: path to store the resulting tables in a private S3 bucket
        
    """
    # filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table =  df.select('song_id', 'artist_id', 'title', 'year', 'duration')\
                            .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('override')\
                .partitionBy('year', 'artist')\
                .parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude'))\
                            .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('override').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """
    Read from S3 and process log data into users table, time table and songplays table.
    
    log data is read from a public S3 bucket, stored as json files. Then, users dataframe,
    time dataframe and songplays dataframe are extracted based on the relevent columns in each.
    Songplays dataframe requires song names and artist names to be replaced with ids.
    Therefore, join is carried out, then columns containing names are dropped.
    Finally, the tables are stored back to a private S3 bucket in parquet fromat.
    Users table is stored under '/users' path, time table is stored under '/time' path
    and songplays table is stored under '/songplays' path.
    
    Parameters:
        spark: Spark session
        input_data: path to lod data in a public S3 bucket
        output_data: path to store the resulting tables in a private S3 bucket
        
    """
    # filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender', 'level')\
                        .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('override').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', from_unixtime(col('ts')/1000))
    
    # extract columns to create time table
    time_table = df.select(col('ts').alias('start_time'), 'timestamp')\
        .withColumn('hour', hour('timestamp'))\
        .withColumn('day', dayofmonth('timestamp'))\
        .withColumn('week', weekofyear('timestamp'))\
        .withColumn('month', month('timestamp'))\
        .withColumn('year', year('timestamp'))\
        .withColumn('weekday', dayofweek('timestamp'))\
        .drop('timestamp')\
        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('override')\
                .partitionBy('year', 'month')\
                .parquet(output_data + 'time')

    # read in song & artist data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs')
    artists_df = spark.read.parquet(output_data + 'artists')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(lit(monotonically_increasing_id()).alias('songplay_id'),
                                     col('ts').alias('start_time'),
                                     col('userId').alias('user_id'),
                                     'level', 'song', 'artist', 'location',
                                     col('sessionId').alias('session_id'),
                                     col('userAgent').alias('user_agent'))\
                                .dropDuplicates()
    
    # extract song name and song id from songs table to be merged with songplays to replace the song name with song id
    song_ids = songs_df.select('song_id', col('title').alias('song'))
    
    # extract artist name and artist id from artists table to be merged with songplays to replace the artist name with artist id
    artist_ids = artists_df.select('artist_id', col('name').alias('artist'))

    # merging song ids and artist ids and droping song names and aritst names
    songplays_table = songplays_table.join(song_ids, songplays_table.song == song_ids.song, 'left')\
                        .drop('song')
    songplays_table = songplays_table.join(artist_ids, songplays_table.artist == artist_ids.artist, 'left')\
                        .drop('artist')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('override')\
                    .partitionBy('year', 'month')\
                    .parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://{}/".format(BUCKET_NAME)
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
