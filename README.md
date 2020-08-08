# Sparkify Datamart on AWS
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We will build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights like what songs their users are listening to.
Since the analytics queries are read heavy, a schema which better suits the faster read performance would be STAR schema as described in the schema design diagram below.Columns marked as **bold** in the diagram are Primary keys for those tables/entities. Lines connecting entities represents Foreign key relationships between tables.

![Sparkify Star Schema](https://github.com/bhosalem/SparkifyAWSDataMart/blob/master/SchemaDiagram.png)

## Architechture :
Refer below the architecture diagram
![Sparkify Architecture](https://github.com/bhosalem/SparkifyAWSDataMart/blob/master/Process%20Flow.PNG)

Note : Diagram drawn using https://www.visual-paradigm.com/features/aws-architecture-diagram-tool/
## 1. Data files:
	There are two data feed files
### Song Data file:
	Type : json
	contains : This file contains data columns for dimensions songs and artists
	Sample Data: {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
### Log data:
	Type : json
	contains : This file contains data columns for dimensions users and time
	Sample Data:
 
![Log data](https://github.com/bhosalem/SparkifyDataWarehouse/blob/bhosalem-patch-1/log-data.png)

## 2. Dimension tables:
dim_songs, dim_users, dim_artists and dim_time are the four dimesion tables as seen n the schema diagram. Refer the [DDL](https://github.com/bhosalem/SparkifyAWSDataMart/blob/master/sql_queries.py) for the columns and datatypes
for each of the dimension tables. All the dimension tables are of type **Slowly Changing Dimension Type-1**. It means that each of the dimensions will maintain only the latest information for the files loaded.
E.g: If User A was opting the for free subscription till 21st April 2019 and switched to paid at a later data then table user will reflect 'level' as 'Paid'. Earlier record for that user gets overwritten to Paid. This avoids any dupliaction as well in the dimensions.

## 3. Fact Table:
The fact table fact_songplays contains dimensional keys from dimensions as well as few events. It essentially is an events table.
E.g Number of users which change to paid subscription in given period of time and vice-versa. This table will be used to derive different insights for anlytics.

# Run Instructions:
1. Run "python create_tables.py" file from command line
   Creates tables for building schema
2. Run "python etl.py" from command line
   Extracts data from song data and log data files stored in S3 and loads them in respective tables in Redshift.
   
**Note : Fill your Cluster details and IAM role details befoer running the scripts.**

# Sample Analytics Queries
## 1. Number of times a song was heard by each user
![Number of time song was heard by each user](https://github.com/bhosalem/SparkifyAWSDataMart/blob/master/user_song_analytics.PNG)



