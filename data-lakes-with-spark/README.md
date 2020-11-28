# Project Description:
Sparkify has stored their user's activities data in S3 - JSON logs and songs. The project needs to build an ETL pipeline in order to extract their data from S3, process them using Spark, and load the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# Project Datasets:
Here are the links to access the data in S3 buckets.
* **Song data**: `s3://udacity-dend/song_data`
* **Log data**: `s3://udacity-dend/log_data`

Sample of datasets:
* **Song data**: 
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
* **Log data**:
```
{"artist":null,"auth":"Logged In","firstName":"Celeste","gender":"F","itemInSession":0,"lastName":"Williams","length":null,"level":"free","location":"Klamath Falls, OR","method":"GET","page":"Home","registration":1541077528796.0,"sessionId":438,"song":null,"status":200,"ts":1541990217796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/37.0.2062.103 Safari\/537.36\"","userId":"53"}
```
# Schema for Song Play Analysis:
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
* **songplays** - records in log data associated with song plays i.e. records with page NextSong
    *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables
* **users** - users in the app
    *user_id, first_name, last_name, gender, level*
    
* **songs** - songs in music database
    *song_id, title, artist_id, year, duration*
    
* **artists** - artists in music database
    *artist_id, name, location, lattitude, longitude*
    
* **time** - timestamps of records in songplays broken down into specific units
    *start_time, hour, day, week, month, year, weekday*

# Prerequisite:
1. Python 3.x.
2. AWS account should be created. S3 bucket should be created and song-data and log-data files should be uploaded into it.
3. Jupyter Notebook is optional. This is to understand and the test your logic of the project. 

# How to run it?
1. Create dwh.cfg configuration file to connect AWS S3 service.
2. Run etl.py either in terminal or in python IDE.

*[Reference: Udacity.com nanodegrees Project - Data Lake]*