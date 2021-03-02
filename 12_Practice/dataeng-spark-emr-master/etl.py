import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, to_date


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    - Create or retrieve existing spark session
    
    Returns: 
        spark -- SparkSession object 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Processes JSON log files stored in input location 
    - Transforms dimension tables: songs, artists
    - Saves output to parquet files 
    
    Arguments: 
        spark -- instatiated object for spark session
        input_data (str) -- path to folder containing log files to be processed
        output_data (str) -- output path for final parquet files 
    """    
    print("Song processing : Started")
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json( song_data )
    
    # extract columns to create songs table
    songs_table = df.select( "song_id", "title", "artist_id", "year", "duration" ).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr( "artist_id", "artist_name as name", "artist_location as location", "artist_latitude as lattitude", "artist_longitude as longitude" )\
        .distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")
    
    print("Song processing : Ended")

def process_log_data(spark, input_data, output_data):
    """
    - Processes JSON log files stored in input location 
    - Transforms dimension tables: users, time
    - Transforms fact tables: songplays
    - Saves output to parquet files 
    
    Arguments: 
        spark -- instatiated object for spark session
        input_data (str) -- path to folder containing log files to be processed
        output_data (str) -- output path for final parquet files 
    """
    
    print("Log processing : Started")
    
    # get filepath to log data file
    log_data = input_data + 'log_data/'

    # read log data file
    df = spark.read.option("recursiveFileLookup","true").json( log_data )

    # filter by actions for song plays
    df = df.filter( col("page") == "NextSong" ) 

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").distinct()

    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf( lambda x : datetime.fromtimestamp( x / 1000 ).strftime( "%Y-%m-%d %H:%M:%S" ) )
    df = df.withColumn( "timestamp", to_timestamp( get_timestamp( "ts" ) ) )

    # create datetime column from original timestamp column
    get_datetime = udf( lambda x : datetime.fromtimestamp( x / 1000 ).strftime( "%Y-%m-%d" ) )
    df = df.withColumn( "date", to_date(get_datetime( "ts" )) )
    
    # extract columns to create time table
    df.createOrReplaceTempView("timetable")
    time_table = spark.sql("""
            SELECT DISTINCT 
                    timestamp AS start_time, 
                    HOUR(timestamp) AS hour, 
                    DAY(timestamp) AS day, 
                    WEEKOFYEAR(timestamp) AS week, 
                    MONTH(timestamp) AS month, 
                    YEAR(timestamp) AS year, 
                    DAYOFWEEK(timestamp) AS weekday
                FROM timetable 
        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet( output_data + "songs.parquet" )
    artist_df = spark.read.parquet( output_data + "artists.parquet" ).selectExpr("artist_id as ref_artist" , "name")
    song_df = song_df.join(artist_df, song_df.artist_id == artist_df.ref_artist )

    if song_df.count() > 0 : 
        # extract columns from joined song and log datasets to create songplays table 
        songplays_table = df.join(song_df , (df.artist == song_df.name) & (df.song == song_df.title) , how='left')\
            .selectExpr("concat_ws('_', userId, ts) as songplay_id", "timestamp as start_time", "userId as user_id", \
                        "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent" )

        # write songplays table to parquet files partitioned by year and month
        songplays_table.withColumn("year", year("start_time")).withColumn("month", month("start_time"))\
            .write.partitionBy("year", "month")\
            .parquet(output_data + "songplays.parquet", mode="overwrite")
    
    print("Log processing : Ended")


def main():
    """
    - Mais ETL function to run on app called
    - Set input and output paths 
    - Run Log and Song processing functions 
    """
    spark = create_spark_session()
    input_data = config['AWS']['INPUT_DATA']
    output_data = config['AWS']['OUTPUT_DATA']
    
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    print("\n ETL Starting\n")
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()
    
    print("\n ETL Complete\n")

if __name__ == "__main__":
    main()
