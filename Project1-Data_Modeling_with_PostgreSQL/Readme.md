# Project 1: Data modeling of a music streaming application using PostgresSQL Database


Scope of this project is to create a PostgresSQL database *Sparkify* with tables designed to optimize queries on song play analysis for a music streaming app. These data are collected from JSON logs (*user activity*) and JSON metadata (*the songs*) in the app. 

## Schema
The star schema designed for this project is showed withint the following image:

![](Schema.png?raw=true)

there is 1 *fact* table:
    *songplays* - records in log data associated with song plays i.e. records with page *NextSong*
     - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

and 4 *dimension* tables:
    *users* - users in the app
     o	*user_id, first_name, last_name, gender, level*
    *songs* - songs in music database
     o	*song_id, title, artist_id, year, duration*
    *artists* - artists in music database
     o	*artist_id, name, location, latitude, longitude*
    *time* - timestamps of records in songplays broken down into specific units
     o	*start_time, hour, day, week, month, year, weekday*

## Files present in this repository

`test.ipynb` - display the first rows of each table present in the database

`create_tables.py` - script that drop/create the database tables and define insert/select statement used

`etl.ipynb` - Jupyter Notebook file used to build the ETL pipeline for each table

`etl.py` - reads and processes all the files and fill the database based on the ETL notebook file.

`README.md` - provide information related to this project.

`sql_queries.py` - Python script with SQL-queries to create and query tables.

# Steps to run the application

In order to run this application, a valid PostgresSQL instance has to be up and running on your environment.

## Step 1 -- Create tables

To create the database **sparkifydb** and the tables, run the file **create_tables.py** from terminal with the following command:
 *python3 create_tables.py*

## Build ETL pipeline

The file **etl.ipynb** present within this repository has been used to create the ETL pipeline. The main parts in this file are the code to process the data in JSON format for the Song Dataset creation and the code to process the data in JSON format for the Log Dataset Creation.

For example, here are filepaths to two files in this dataset.
*song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json*

And below is an example of what a single song file, *TRAABJL12903CDCF1A.json*, looks like.

*{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}*

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

*log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json*

And below is an example of what the data in a log file, *2018-11-12-events.json*, looks like.

![](events.png?raw=true)

To check the result, run the file *test.ipynb* on Jupyter Notebook.

## Main process etl.py

The file **etl.py** (based on **etl.ipynb** file), process, extract, convert and populate the tables *songs* and *artists* with from the JSON song files, `data/song_data` while the tables **time** and **users** are populated from the JSON log files, `data/log_data`. In order to populate the **songplays** fact table, a `SELECT` statement has been used to collects *song_id* and *artist_id* from the tables **songs** and **artists** combining with log file. 






