create_tables.py                                                                                    0000644 0000000 0000000 00000001750 14055751363 012737  0                                                                                                    ustar   root                            root                                                                                                                                                                                                                   import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
      This function is used in order to drop all the tables configured withion sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
      This function is used in create tables within sql_queries.py
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
          
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()                        delete_cluster.py                                                                                   0000644 0000000 0000000 00000005054 14056041035 013133  0                                                                                                    ustar   root                            root                                                                                                                                                                                                                   #!/usr/bin/env python
# coding: utf-8

# # File to delete Redshift Cluster using the AWS python SDK (IAC)

import pandas as pd
import boto3
#import json

import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

PRO_CLUSTER_IDENTIFIER = config.get("PRO","PRO_CLUSTER_IDENTIFIER")
PRO_IAM_ROLE_NAME      = config.get("PRO", "PRO_IAM_ROLE_NAME")



def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def main():
    

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )
    
    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    # Delete resources
    try:
        redshift.delete_cluster(ClusterIdentifier=PRO_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    except Exception as e:
        print("Cluster {} not found!".format(PRO_CLUSTER_IDENTIFIER))
    
    result = False
    
    while not result:
        try:
        # Check if the cluster has been deleted 
            ClusterProps = redshift.describe_clusters(ClusterIdentifier=PRO_CLUSTER_IDENTIFIER)['Clusters'][0]
            prettyRedshiftProps(ClusterProps)
            result = True
            print('Check cluster deletetion!!')
        except Exception as e:
            print("Cluster {} cannot be deleted because not found!".format(PRO_CLUSTER_IDENTIFIER))
            break
        
    try:    
        iam.detach_role_policy(RoleName=PRO_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=PRO_IAM_ROLE_NAME)
        print('IAM deleted')
    except Exception as e:
        print("IAM role {} cannot be deleted because not found!".format(PRO_IAM_ROLE_NAME))
    

if __name__ == "__main__":
    main()

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    dwh.cfg                                                                                             0000644 0000000 0000000 00000000615 14056056024 011023  0                                                                                                    ustar   root                            root                                                                                                                                                                                                                   [CLUSTER]
HOST=
DB_NAME=project_final
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=
SECRET=

[PRO] 
PRO_CLUSTER_TYPE=multi-node
PRO_NUM_NODES=4
PRO_NODE_TYPE=dc2.large
PRO_IAM_ROLE_NAME=proRolefinal
PRO_CLUSTER_IDENTIFIER=finalprojectclusteraws


                                                                                                                   etl.py                                                                                              0000644 0000000 0000000 00000003555 14056043502 010721  0                                                                                                    ustar   root                            root                                                                                                                                                                                                                   import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_count_rows_tables


def load_staging_tables(cur, conn):
    # Function that load the event and songs files into staging tables
    # Function name: load_staging_tables
    # Parameters: conn for database connection
    #             cur   
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    # Function that load the data into fact and dimension tables
    # Function name: insert_tables
    # Parameters: conn for database connection
    #             cur  
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def count_rows(cur, conn):
    # Function that print the number of rows stored in each table
    # Function name: count_rows
    # Parameters: conn for database connection
    #             cur  
    for query in select_count_rows_tables:

        cur.execute(query)
        results = cur.fetchone()
        table_name = query.split()

        for row in results:
            print("Total score for {} is {}".format(table_name[5], row))
            

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except psycopg2.Error as e:
        print("Unable to connect!")
        print(e.pgerror)
        print(e.diag.message_detail)

    print("Connected!")
     
    try:
        cur = conn.cursor()
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        print("***********************************************")
        count_rows(cur, conn)
        conn.close()
    except psycopg2.Error as e:
        print(e)

if __name__ == "__main__":
    main()                                                                                                                                                   __pycache__/                                                                                        0000755 0000000 0000000 00000000000 14056055314 011767  5                                                                                                    ustar   root                            root                                                                                                                                                                                                                   __pycache__/sql_queries.cpython-36.pyc                                                              0000644 0000000 0000000 00000013555 14056055314 016762  0                                                                                                    ustar   root                            root                                                                                                                                                                                                                   3
?Z?`?  ?               @   s  d dl Z e j? Zejd? dZdZdZdZdZdZ	d	Z
d
ZdZdZdZdZdZdZdjed d ed d ed d ?Zdjed d ed d ?ZdZdZdZdZdZdZdZd Zd!Zd"ZeeeeeeegZeeeeee	e
gZ eegZ!eeeeegZ"eeeeegZ#dS )#?    Nzdwh.cfgz#DROP TABLE IF EXISTS staging_eventsz"DROP TABLE IF EXISTS staging_songsz DROP TABLE IF EXISTS f_songplayszDROP TABLE IF EXISTS d_userszDROP TABLE IF EXISTS d_songszDROP TABLE IF EXISTS d_artistszDROP TABLE IF EXISTS d_timea2  
   CREATE TABLE staging_events (
       artist       VARCHAR,
       auth         VARCHAR,
       firstname    VARCHAR,
       gender       VARCHAR,
       item_session INTEGER,
       lastname     VARCHAR ,
       length       VARCHAR,
       level        VARCHAR,
       location     VARCHAR,
       method       VARCHAR,
       page         VARCHAR,
       registration VARCHAR,
       sessionid    INTEGER,
       song         VARCHAR ,
       status       INTEGER,
       ts           BIGINT,
       useragent    VARCHAR,
       userid       INTEGER
   );
aj  
   CREATE TABLE staging_songs (
       num_songs        INTEGER,
       artist_id        VARCHAR,
       artist_latitude  FLOAT,
       artist_longitude FLOAT,
       artist_location  VARCHAR,
       artist_name      VARCHAR,
       song_id          VARCHAR,
       title            VARCHAR,
       duration         FLOAT,
       year             INTEGER
   );
a?  
   CREATE TABLE f_songplays (
       songplay_id    INTEGER IDENTITY(0,1) PRIMARY KEY, 
       start_time     TIMESTAMP sortkey NOT NULL, 
       user_id        INTEGER distkey NOT NULL, 
       level          VARCHAR, 
       song_id        VARCHAR, 
       artist_id      VARCHAR, 
       session_id     INTEGER, 
       location       VARCHAR, 
       user_agent     VARCHAR
   );
z?
   CREATE TABLE d_users (
       user_id    INTEGER PRIMARY KEY distkey, 
       first_name VARCHAR, 
       last_name  VARCHAR, 
       gender     VARCHAR(1), 
       level      VARCHAR
   );
z?
   CREATE TABLE d_songs (
       song_id   VARCHAR PRIMARY KEY, 
       title     VARCHAR sortkey, 
       artist_id VARCHAR distkey NOT NULL, 
       year      INTEGER, 
       duration  FLOAT
   );
z?
   CREATE TABLE d_artists (
       artist_id VARCHAR PRIMARY KEY, 
       name      VARCHAR, 
       location  VARCHAR, 
       latitude  VARCHAR, 
       longitude VARCHAR
   );   
a  
   CREATE TABLE d_time (
       start_time VARCHAR PRIMARY KEY sortkey distkey, 
       hour       INTEGER, 
       day        INTEGER, 
       week       INTEGER, 
       month      INTEGER, 
       year       INTEGER, 
       weekday    INTEGER
   );    
z?
   COPY staging_events 
   FROM {}
   CREDENTIALS 'aws_iam_role={}'
   COMPUPDATE OFF region 'us-west-2'
   STATUPDATE ON
   FORMAT AS JSON {} ;
ZS3ZLOG_DATAZIAM_ROLEZARNZLOG_JSONPATHz?
   COPY staging_songs
   FROM {}
   CREDENTIALS 'aws_iam_role={}'
   COMPUPDATE OFF region 'us-west-2'
   TIMEFORMAT as 'epochmillisecs'
   STATUPDATE ON
   format as JSON 'auto' ;
Z	SONG_DATAa;  
   INSERT INTO f_songplays (start_time, song_id, user_id, level, artist_id, session_id, location, user_agent) 
   SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
         ss.song_id as song_id,     
         se.userid as user_id,
         se.level as level,
         ss.artist_id as artist_id,
         se.sessionid as session_id,
         se.location as location,
         se.useragent as user_agent
   FROM staging_events se
   JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name) AND se.page = 'NextSong';
a9  
   INSERT INTO d_users (user_id, first_name, last_name, gender, level) 
   SELECT DISTINCT
         userid as user_id,
         firstname as first_name,
         lastname as last_name,
         gender as gender,
         level as level
   FROM staging_events 
   WHERE userid is not null and page = 'NextSong'; 
z?
   INSERT INTO d_songs (song_id, title, artist_id, year, duration) 
   SELECT DISTINCT
         song_id,
         title,
         artist_id,
         year,
         duration
   FROM  staging_songs;
z?
   INSERT INTO d_artists (artist_id, name, location, latitude, longitude) 
   SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
   FROM staging_songs;   
a?  
   INSERT INTO d_time (start_time, hour, day, week, month, year, weekday)
   WITH time_source AS
(
    SELECT
        DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events
)
   SELECT start_time as start_time,
       EXTRACT(hour from start_time),
       EXTRACT(day from start_time),
       EXTRACT(week from start_time),
       EXTRACT(month from start_time),
       EXTRACT(year from start_time),
       EXTRACT(weekday from start_time)
   FROM time_source;
z-SELECT COUNT(*) as total_row FROM f_songplaysz)SELECT COUNT(*) as total_row FROM d_usersz)SELECT COUNT(*) as total_row FROM d_songsz+SELECT COUNT(*) as total_row FROM d_artistsz(SELECT COUNT(*) as total_row FROM d_time)$?configparser?ConfigParser?config?readZstaging_events_table_dropZstaging_songs_table_dropZsongplay_table_dropZuser_table_dropZsong_table_dropZartist_table_dropZtime_table_dropZstaging_events_table_createZstaging_songs_table_createZsongplay_table_createZuser_table_createZsong_table_createZartist_table_createZtime_table_create?formatZstaging_events_copyZstaging_songs_copyZsongplay_table_insertZuser_table_insertZsong_table_insertZartist_table_insertZtime_table_insertZnum_rows_f_songplaysZnum_rows_d_usersZnum_rows_d_songsZnum_rows_d_artistsZnum_rows_d_time?create_table_queries?drop_table_queriesZcopy_table_queriesZinsert_table_queriesZselect_count_rows_tables? r	   r	   ?/home/workspace/sql_queries.py?<module>   sB   



&
                                                                                                                                                   README.md                                                                                           0000644 0000000 0000000 00000005160 14056040360 011033  0                                                                                                    ustar   root                            root                                                                                                                                                                                                                   ## Summary

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
                                                                                                                                                                                                                                                                                                                                                                                                                sql_queries.py                                                                                      0000644 0000000 0000000 00000015331 14056055374 012476  0                                                                                                    ustar   root                            root                                                                                                                                                                                                                   import configparser

# Load CONFIG parameters and credential from file 

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS f_songplays"
user_table_drop = "DROP TABLE IF EXISTS d_users"
song_table_drop = "DROP TABLE IF EXISTS d_songs"
artist_table_drop = "DROP TABLE IF EXISTS d_artists"
time_table_drop = "DROP TABLE IF EXISTS d_time"


# CREATE TABLES 
# Staging Table that is going to load all the events coming from the events-json files

staging_events_table_create= ("""
   CREATE TABLE staging_events (
       artist       VARCHAR,
       auth         VARCHAR,
       firstname    VARCHAR,
       gender       VARCHAR,
       item_session INTEGER,
       lastname     VARCHAR ,
       length       VARCHAR,
       level        VARCHAR,
       location     VARCHAR,
       method       VARCHAR,
       page         VARCHAR,
       registration VARCHAR,
       sessionid    INTEGER,
       song         VARCHAR ,
       status       INTEGER,
       ts           BIGINT,
       useragent    VARCHAR,
       userid       INTEGER
   );
""")

# Staging Table that is about a song and the artist of that song.  

staging_songs_table_create = ("""
   CREATE TABLE staging_songs (
       num_songs        INTEGER,
       artist_id        VARCHAR,
       artist_latitude  FLOAT,
       artist_longitude FLOAT,
       artist_location  VARCHAR,
       artist_name      VARCHAR,
       song_id          VARCHAR,
       title            VARCHAR,
       duration         FLOAT,
       year             INTEGER
   );
""")

# CREATE FACT TABLE and DIMENSIONAL TABLE

songplay_table_create = ("""
   CREATE TABLE f_songplays (
       songplay_id    INTEGER IDENTITY(0,1) PRIMARY KEY, 
       start_time     TIMESTAMP sortkey NOT NULL, 
       user_id        INTEGER distkey NOT NULL, 
       level          VARCHAR, 
       song_id        VARCHAR, 
       artist_id      VARCHAR, 
       session_id     INTEGER, 
       location       VARCHAR, 
       user_agent     VARCHAR
   );
""")

user_table_create = ("""
   CREATE TABLE d_users (
       user_id    INTEGER PRIMARY KEY distkey, 
       first_name VARCHAR, 
       last_name  VARCHAR, 
       gender     VARCHAR(1), 
       level      VARCHAR
   );
""")

song_table_create = ("""
   CREATE TABLE d_songs (
       song_id   VARCHAR PRIMARY KEY, 
       title     VARCHAR sortkey, 
       artist_id VARCHAR distkey NOT NULL, 
       year      INTEGER, 
       duration  FLOAT
   );
""")

artist_table_create = ("""
   CREATE TABLE d_artists (
       artist_id VARCHAR PRIMARY KEY, 
       name      VARCHAR, 
       location  VARCHAR, 
       latitude  VARCHAR, 
       longitude VARCHAR
   );   
""")

time_table_create = ("""
   CREATE TABLE d_time (
       start_time VARCHAR PRIMARY KEY sortkey distkey, 
       hour       INTEGER, 
       day        INTEGER, 
       week       INTEGER, 
       month      INTEGER, 
       year       INTEGER, 
       weekday    INTEGER
   );    
""")

# STAGING TABLES loading phase from files

staging_events_copy = ("""
   COPY staging_events 
   FROM {}
   CREDENTIALS 'aws_iam_role={}'
   COMPUPDATE OFF region 'us-west-2'
   STATUPDATE ON
   FORMAT AS JSON {} ;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
   COPY staging_songs
   FROM {}
   CREDENTIALS 'aws_iam_role={}'
   COMPUPDATE OFF region 'us-west-2'
   TIMEFORMAT as 'epochmillisecs'
   STATUPDATE ON
   format as JSON 'auto' ;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# Fact and dimensional TABLES populated from staging tables

songplay_table_insert = ("""
   INSERT INTO f_songplays (start_time, song_id, user_id, level, artist_id, session_id, location, user_agent) 
   SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
         ss.song_id as song_id,     
         se.userid as user_id,
         se.level as level,
         ss.artist_id as artist_id,
         se.sessionid as session_id,
         se.location as location,
         se.useragent as user_agent
   FROM staging_events se
   JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name) AND se.page = 'NextSong';
""")

user_table_insert = ("""
   INSERT INTO d_users (user_id, first_name, last_name, gender, level) 
   SELECT DISTINCT
         userid as user_id,
         firstname as first_name,
         lastname as last_name,
         gender as gender,
         level as level
   FROM staging_events 
   WHERE userid is not null and page = 'NextSong'; 
""")

song_table_insert = ("""
   INSERT INTO d_songs (song_id, title, artist_id, year, duration) 
   SELECT DISTINCT
         song_id,
         title,
         artist_id,
         year,
         duration
   FROM  staging_songs;
""")

artist_table_insert = ("""
   INSERT INTO d_artists (artist_id, name, location, latitude, longitude) 
   SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
   FROM staging_songs;   
""")

time_table_insert = ("""
   INSERT INTO d_time (start_time, hour, day, week, month, year, weekday)
   WITH time_source AS
(
    SELECT
        DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events
)
   SELECT start_time as start_time,
       EXTRACT(hour from start_time),
       EXTRACT(day from start_time),
       EXTRACT(week from start_time),
       EXTRACT(month from start_time),
       EXTRACT(year from start_time),
       EXTRACT(weekday from start_time)
   FROM time_source;
""")


## Querying the number of rows for each table that the data was inserted into
num_rows_f_songplays = ("""SELECT COUNT(*) as total_row FROM f_songplays""")
num_rows_d_users =     ("""SELECT COUNT(*) as total_row FROM d_users""")
num_rows_d_songs =     ("""SELECT COUNT(*) as total_row FROM d_songs""")
num_rows_d_artists =   ("""SELECT COUNT(*) as total_row FROM d_artists""")
num_rows_d_time =      ("""SELECT COUNT(*) as total_row FROM d_time""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_count_rows_tables= [num_rows_f_songplays, num_rows_d_users, num_rows_d_songs, num_rows_d_artists, num_rows_d_time]
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       