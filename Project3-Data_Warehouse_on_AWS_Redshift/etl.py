# Importing library to use in this ETL pipeline

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as tempo

# Reading config file
config = configparser.ConfigParser()
config.read('dl.cfg')

def create_spark_session():
    """
        Create a new Spark Session with hadoop plugin
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data, run_start_time):

    # read song data file
    print("\nReading SONG DATA JSON files from {}...".format(input_data))
    df = spark.read.json(input_data) 
    print("\n... finished reading SONG DATA JSON files from {}...".format(input_data))
   # print("Print song_data Schema!!!")
    #df.printSchema()
    
    # Let's create a temporary view to manipolate the song_data
    print("\nCreating writing SONGS TABLE.")
    df.createOrReplaceTempView("song_table_view")
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT song_id,
               title, 
               artist_id,
               year,
               duration
        FROM song_table_view
        ORDER BY song_id
    """)
    print("\nSONGS TABLE:")
    songs_table.show(5, truncate=False)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_output = output_data + "songs_table.parquet" + "-" + run_start_time
    print("\nPrint songs table to parquet files {}...".format(songs_table_output))
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(songs_table_output)
    
    print("\n...finished writing SONGS TABLE.")
    # extract columns to create artists table
    print("\nCreating ARTISTS TABLE")
    df.createOrReplaceTempView("artist_table_view")
    artists_table = spark.sql("""
        SELECT artist_id         AS artist_id,
               artist_name       AS name,
               artist_location   AS location, 
               artist_latitude   AS latitude,
               artist_longitude  AS longitude,
               year
        FROM artist_table_view
        ORDER BY artist_id
    """)
    print("\nARTIST TABLE:")
    artists_table.show(5, truncate=False)
    
    # write artists table to parquet files
    artists_table_output = output_data + "artists_table.parquet" + "-" + run_start_time
    print("\nPrint ARTISTS TABLE to parquet files {}...".format(artists_table_output))
    artists_table.write.mode("overwrite").parquet(artists_table_output)
    
    print("\n...finished writing ARTISTS TABLE.")
    

def process_log_data(spark, input_song_data, input_log_data, output_data, run_start_time):
    
    # get filepath to log and song data files
    log_data = input_log_data
    song_data = input_song_data
    
     # read log data file
    print("\nReading LOG DATA JSON files from {}...".format(log_data))
    df_log = spark.read.json(log_data) 
    print("\n... finished reading LOG DATA JSON files from {}...".format(log_data))
    #print("\nPrint log_data Schema!!!")
    #df_log.printSchema()
    
    # read song data file
    print("\Reading SONG DATA JSON files from {}...".format(song_data))
    df_song = spark.read.json(song_data) 
    print("\n... finished reading SONG DATA JSON files from {}...".format(song_data))
    #print("Print song_data Schema!!!")
    #df_song.printSchema()
    #df_song.show()
    
    # We are now going to filter the data with dataframe df using as reference the field page = 'NextSong' -- filter by actions for song plays
    df_fil = df_log.filter(df_log.page == 'NextSong')
    
    # Let's create a temporary view to manipolate the log_data
    # extract columns for users table  
    print("\nCreating USERS TABLE.")
    df_fil.createOrReplaceTempView("users_table")

    users_table = spark.sql("""
        SELECT DISTINCT userId as user_id,
               firstName as first_name, 
               lastName as last_name,
               gender,
               level
        FROM users_table
        ORDER BY user_id
    """)
    print("USERS TABLE:")
    users_table.show(5, truncate=False) 
    
    # write users table to parquet files
    users_table_output = output_data + "users_table.parquet" + "-" + run_start_time
    print("\nPrint users table to parquet files {}...".format(users_table_output))
    users_table.write.mode("overwrite").parquet(users_table_output)
    
    print("\n...finished writing USERS TABLE.")

    # create timestamp column from original timestamp column
    print("\nCreate timestamp column .... ")
        
    @udf(tempo.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts/1000.0)

    df_fil = df_fil.withColumn("timestamp", get_timestamp("ts"))
    #df_fil.printSchema()
    df_fil.show(5, truncate=False)
    

    # create datetime column from original timestamp column
    print("\nCreate datetime column .... ")

    @udf(tempo.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    df_fil = df_fil.withColumn("datetime", get_datetime("ts"))
   # df_fil.printSchema()
    df_fil.show(5, truncate=False)

    # extract columns to create time table
    
    print("\nCreate TIME TABLE .... ")
    
    df_fil.createOrReplaceTempView("time_table")
    time_table = spark.sql("""
            SELECT DISTINCT datetime as start_time,
                            hour(timestamp) as hour,
                            day(timestamp) as day,
                            weekofyear(timestamp) as week,
                            month(timestamp) as month,
                            year(timestamp) as year,
                            dayofweek(timestamp) as weekday
             FROM time_table
             ORDER by start_time
    """)
   # print("time_table Schema:")
   # time_table.printSchema()
    print("\nTIME TABLE:")
    time_table.show(5, truncate=False)
    
    # write time table to parquet files partitioned by year and month

    time_table_output = output_data + "time_table.parquet" + "-" + run_start_time
    print("\nPrint time table to parquet files {}...".format(time_table_output))
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(time_table_output)
    print("\n...finished writing TIME TABLE.")
    
    # Merging song data and Log data to use for songplays table
    
    print("\nMerging LOG DATA and SONG DATA ...")
    df_merged = df_fil.join(df_song, (df_fil.song == df_song.title))
    print("\n ...finished merge between LOG DATA and SONG DATA.")
    #print("\nMerged song_data and log_data schema:")
    #df_merged.printSchema()
    print("\nLOG DATA and SONG DATA Examples:")
    df_merged.show(5, truncate=False)
    
    # extract columns from joined song and log datasets to create songplays table 
    print("\nExtracting columns from merged song and log datasets...")
    df_merged = df_merged.withColumn("songplay_id", monotonically_increasing_id())
    df_merged.createOrReplaceTempView("songplays_table")
    songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id,
                timestamp   AS start_time,
                userId      AS user_id,
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM songplays_table
        ORDER BY (user_id, session_id)
    """)
    #print("\nSONGPLAYS TABLE schema:")
    #songplays_table.printSchema()
    print("\nSONGPLAYS TABLE examples:")
    songplays_table.show(5, truncate=False)
    # write songplays table to parquet files partitioned by year and month
    #songplays_table
    songplays_table_output = output_data + "songplays_table.parquet" + "-" + run_start_time
    print("Print songplays table to parquet files {}...".format(songplays_table_output))
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(songplays_table_output)
    
    print("\n...finished writing SONGPLAYS TABLE.")
    


def main():
    """
       This is the main part of the python program.
       
       
    """
    

    start_date_time = datetime.now()
    run_start_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    print("\nSTARTED ETL pipeline at {}\n".format(start_date_time))
    print("\nCreating Spark Session ... ")
    spark = create_spark_session()
    
    # input_song_data = "s3a://udacity-dend/"
    # output_data = ""
    
    # Variables to set when we are processing data using  S3.

    #os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    #os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    # input_song_data = config['AWS']['INP_DATA_SONG_S3']
    # input_log_data = config['AWS']['INP_DATA_LOG_S3']
    # output_data = config['AWS']['OUT_DATA_S3']

    
    # The data in json format within song_data folder are organized on 3 level
    
    # /song_data
    #     |_/A
    #        |_/A
    #          |_/A
    #            | *.json

    # The data in json format within log-data folder are organized on 1 level

    # /log-data
    #     |*.json 
    
    # These variables are used for Local purposes
    
    input_song_data = config['LOCAL']['INP_DATA_SONG_L']
    input_log_data  = config['LOCAL']['INP_DATA_LOG_L']
    output_data     = config['LOCAL']['OUT_DATA_L']
    
    # Use AWS input_data + output_data paths.
    print("\n CALLING process_song_data function.")
    process_song_data(spark, input_song_data, output_data, run_start_time)
    print("\n CALLING process_log_data function.")
    process_log_data(spark, input_song_data, input_log_data, output_data, run_start_time)
    
    print("Finished the ETL pipeline processing.")

if __name__ == "__main__":
    main()    
