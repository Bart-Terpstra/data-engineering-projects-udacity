# Project 1b: Data Modeling with Cassandra

## Project Introduction
Startup Sparkify wants to analyze the data from collected from its music streaming app. The data consists of songs and user activity on the app. They mainly want to understand to what songs the users are listening to. For this purpose, Sparkify wants a Apache Cassandra database optimized for queries to perform songplay analysis. I designed a Cassandra database and ETL pipeline. The database contains all relevant data stored in 3 tables optimized to execute 3 queries.

## Queries

1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


## Data
The data consists of CSV files on user activity on the app. These are stored in the folder `event_data`. A sample of the data is shown in the image below.

![Sample event data](images/image_event_datafile_new.jpg "Sample event data")


## Modeling and ETL
The Jupyter Notebook `data_modeling_with_cassandra.ipynb` executes the following steps:
1. CSV files are preprocessed into a single CSV called `event_datafile_new.csv`
2. The Cassandra cluster and keyspace 'sparkify' are created
3. For each query, a specific table is created on the keyspace
4. The (relevant) data from `event_datafile_new.csv` is inserted in the three tables
5. Queries are created to select data from the tables.
