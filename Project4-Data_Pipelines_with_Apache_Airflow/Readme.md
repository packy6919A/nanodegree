# Project: Data Pipeline with Airflow
## About this Project
A music streaming company, Sparkify, wants to introduce more automation and monitoring to their data warehouse ETL pipelines through Apache Airflow.
To do that, data pipelines with **Apache Airflow** have been scheduled and setup to extract raw datasets from **S3** to **Redshift**. The raw data extract from **S3** are going to be transformed and loaded from staging tables to dimensional tables. We also monitor production pipelines by run data quality checks to track data linage.


## Project Structure

```
Data Pipeline with Apache Airflow
|
|____dags
| |____ s3_to_redshift_dag.py    # This is the DAG for this ETL data pipeline
|
|____plugins
| |____operators
| | |____ stage_redshift.py    # This application copy data from S3 to Redshift
| | |____ load_fact.py         # This application execute INSERT query into fact table
| | |____ load_dimension.py    # This application execute INSERT queries into dimension tables
| | |____ data_quality.py      # This application makes data quality check after pipeline execution
| |
| |____helpers
| | |____ sql_queries.py       # SQL queries for building dimensional tables
```

## How to execute
1. First of all you need to create a Redshift cluster on AWS account and provide the parameters connection parameters 
2. Run Airflow using the command /opt/airflow/start.sh

## Airflow Data Pipeline

### Operators

1. `Begin_execution` & `End_execution`

    Dummy operators at data pipeline end points

2. `Stage_events` & `Stage_songs`

    Extract/Transform data from S3 to Redshift to create staging tables

3. `Load_songplays_fact_table` & `Load_*_dim_table`

    Load data from staging tables to dimensional tables

4. `data_quality.py` 

    Check no empty table after data loading. More tests can be added into this operator to ensure data quality

