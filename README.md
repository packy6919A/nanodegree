# Data Engineering Nanodegree Course

Projects and resources developed in the [DEND Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) from Udacity.

## Project 1: [Relational Databases - Data Modeling with PostgreSQL](https://github.com/packy6919A/nanodegree/tree/main/Project1-Data_Modeling_with_PostgreSQL).
In this project, PostgreSQL relational database has been developed to model user activity data for a music streaming app.
Activities done include:
 
* A database have been created using PostgreSQL.
* Developed a Star Schema database using optimized definitions of Fact and Dimension tables. Normalization of tables.
* Create an ETL pipeline to optimize queries in order to understand what songs the users are listen.

Technology used: Python, PostgreSql, Star Schema, ETL pipelines, Normalization

## Project 2: [NoSQL Databases - Data Modeling with Apache Cassandra](https://github.com/packy6919A/nanodegree/tree/main/Project2-Data_Modeling_with_Apache_Cassandra).
In this project, a NoSQL database has been designed using Apache Cassandra based on the original schema outlined in project one. 
Activities done include:

* A nosql database have been created using Apache Cassandra 
* Developed denormalized tables optimized for a specific set queries and business needs

Technology used: Python, Apache Cassandra, Denormalization

## Project 3: [Data Lake - Spark](https://github.com/packy6919A/nanodegree/tree/main/Project3-Data_Lake_on_AWS_S3)
Scaled up the current ETL pipeline by moving the data warehouse to a data lake.
Activities done include: 

* A EMR Hadoop Cluster have been developed.
* An ETL Pipeline have been developed copy datasets from S3 buckets. The data have been processed using Spark to write in S3 buckets using efficient partitioning and parquet formatting.
* Fast-tracking the data lake buildout using (serverless) AWS Lambda and cataloging tables with AWS Glue Crawler.

Technologies used: Spark, S3, EMR, Athena, Amazon Glue, Parquet.

## Project 4: [Data Pipelines - Airflow](https://github.com/packy6919A/nanodegree/tree/main/Project4-Data_Pipelines_with_Apache_Airflow)
Automate the ETL pipeline and creation of data warehouse using Apache Airflow.
Activities done include:

* Using Airflow to automate ETL pipelines using Airflow, Python, Amazon Redshift.
* Writing custom operators to perform tasks such as staging data, filling the data warehouse, and validation through data quality checks.
* Transforming data from various sources into a star schema optimized for the analytics team's use cases.

Technologies used: Apache Airflow, S3, Amazon Redshift, Python.
 
