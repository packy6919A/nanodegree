## Summary

Scope of this project is to build an ETL pipeline that extracts data from S3 to staging tables on Redshift, and transforms data into a set of dimensional tables.

The project is written in `python` and uses `Amazon s3` for file storage and `Amazon Redshift` for database storage and data warehouse purpose.

## Source Data

The source data are log files stored in Amazon S3 bucket at `s3://udacity-dend/log_data` and `s3://udacity-dend/song_data` that contain respectively user songplay events in JSON format and songs details.

## Schema for Song Play Analysis

Following are the fact and dimension tables of this project:

#### Fact Table:

   * f_songplays - records in event data associated with song plays i.e. records with page NextSong
        * columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables:

   * d_users - users in the app
        * columns: user_id, first_name, last_name, gender, level
   * d_songs - songs in music database
        * columns: song_id, title, artist_id, year, duration
   * d_artists - artists in music database
        * columns: artist_id, name, location, lattitude, longitude
   * d_time - timestamps of records in songplays broken down into specific units
        * columns: start_time, hour, day, week, month, year, weekday

## Project template
This project includes 7 files:
   
   * `README.md` - include the information how to run the application and explanation of structure.
   * `create_cluster.py` - python file that use the `dwh.cfg` configuration file to create the AWS cluster, IAM rules.
   * `create_table.py` - python file that re-call `sql_queries.sql` file to create table structures and load Fact tablea and dimension tables.
   * `delete_cluster.py` - python file used to delete cluster and related resources.
   * `dwh.cfg` - Configuration file.
   * `sql_queries.py` - python file that contain the definition for tables creation, insert statements in these tables and select statement to count the number of rows inserted.
   * `etl.py` - python file used to load data from S3 into staging tables on Redshift and then these data into the abalytics tables.
   
   
## Run the application:

   * Update the `dwh.cfg` file with the Amazon Redshift cluster credentials and IAM role that can access the cluster.
   * Run `python3 create_cluster.py` if the cluster does not exists. 
   * Run `python3 delete_cluster.py` if the cluster should be dropped with related IAM roles.
   * Run `python3 create_tables.py` to create the tables.
   * Run `python3 etl.py` start the pipeline to read the data from files and populate the tables.
