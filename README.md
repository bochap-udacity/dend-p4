# Project: Data Lake

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Running this code

### Prerequisites

1. Python 3.6 and above
2. GIT setup and configured for SSH
3. Docker (If running locally)

### Files
THe following shows the file in the repository
```
├── Dockerfile              ' File to create the Docker Image for Apache Spark
├── LICENSE
├── README.md               ' File describing the projects
├── data                    ' Folder to store data for running locally
├── data_analysis.ipynb     ' Data Analysis document to study data
├── dl.cfg.template         ' Configuration file if running against AWS S3  
└── etl.py                  ' Code file for project
```

### Running locally
1. Clone repository by running `git clone git@github.com:seetdev/dend-p4.git`
2. Go into the cloned folder
3. Create folders for `data/input` and `data/output`
4. Download log-data and song_data from `s3a://udacity-dend` into `data/input`
5. Setup the docker image by running `docker build --tag udacity-dend/pyspark-notebook .`
6. Start docker container by runnin `docker run --rm -d -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes -v $PWD:/home/jovyan/work --name spark udacity-dend/pyspark-notebook`
7. In `etl.py` comment the block just below `## S3 block` and uncomment the block between `## Local block` and `## S3 block`
8. Run `python ./etl.py`

### Running against AWS S3
1. Clone repository by running `git clone git@github.com:seetdev/dend-p4.git`
2. Go into the cloned folder
3. Create the output bucket in S3
4. Renamed `dl.cfg.template` to `dl.cfg` and fill up the variables
5. Setup the docker image by running `docker build --tag udacity-dend/pyspark-notebook .`
6. Start docker container by runnin `docker run --rm -d -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes -v $PWD:/home/jovyan/work --name spark udacity-dend/pyspark-notebook`
7. In `etl.py` uncomment the block just below `## S3 block` and comment the block between `## Local block` and `## S3 block`
8. Run `python ./etl.py`

## Transformed Schemas

### songs_table schema:
```
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
```

### artists_table schema:
```
root
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```

### users_table schema:
```
root
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```

### time_table schema:
```
root
 |-- start_time: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: string (nullable = true)
```

### songplays_table schema:
```
root
 |-- start_time: timestamp (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```
