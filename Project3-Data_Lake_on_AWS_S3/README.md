# Project 3: Data Lake

Scope of this projectis is to process a large amount of data related to a music streaming service named Sparkify.

## Problem

The scenario is that Sparkify has grown their user base and song database and they want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We have two datasets that reside in S3:

  - *Song data:* `s3://udacity-dend/song_data`
  - *Log data:*  `s3://udacity-dend/log_data`

## Solution to the problem

We are going to read the song and log datasets using (Py)Spark and transform into a star schema optimized for queries on song play analysis. Finally we store the results as files from which they can be easily processed later on.

## Getting started

To execute the `etl.py` script, which can either be executed in a local mode or the remote mode some changes are needed within the `etl.py` and `dl.cfg` files.

## Run in local mode:
Make sure to first unzip the sample data under a data folder following the structure:

``` 
main_folder/ 
+-- etl.py
+-- dl.cfg
+-- README.md
+-- data/
    +-- log-data.zip
    +-- song_data.zip
```
In the `def main()` function, uncomment the following parameters:

- `input_song_data = config['LOCAL']['INP_DATA_SONG_L']`
- `input_log_data  = config['LOCAL']['INP_DATA_LOG_L']`
- `output_data     = config['LOCAL']['OUT_DATA_L']`

and comment the following lines:

- `os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']`
- `os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']`
- `input_song_data = config['AWS']['INP_DATA_SONG_S3']`
- `input_log_data = config['AWS']['INP_DATA_LOG_S3']`
- `output_data = config['AWS']['OUT_DATA_S3']`

and check within `dl.cfg` file that the paths are correct. 

## Remote mode (S3) make sure to enter AWS credentials in the dl.cfg file first

After verified the presence of the data files, in the `def main()` function, comment the following parameters:

- `input_song_data = config['LOCAL']['INP_DATA_SONG_L']`
- `input_log_data  = config['LOCAL']['INP_DATA_LOG_L']`
- `output_data     = config['LOCAL']['OUT_DATA_L']`

and uncomment the following lines:

- `os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']`
- `os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']`
- `input_song_data = config['AWS']['INP_DATA_SONG_S3']`
- `input_log_data = config['AWS']['INP_DATA_LOG_S3']`
- `output_data = config['AWS']['OUT_DATA_S3']`

Check within `dl.cfg` file that the paths are correct.

## Command line to tun the application
`python3 etl.py`
