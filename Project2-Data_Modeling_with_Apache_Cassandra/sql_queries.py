import configparser

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
