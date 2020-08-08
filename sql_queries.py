import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = \
    'DROP TABLE IF EXISTS staging.events CASCADE;'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging.songs CASCADE;'
songplay_table_drop = 'DROP TABLE IF EXISTS dwh.songplay CASCADE;'
user_table_drop = 'DROP TABLE IF EXISTS dwh.users CASCADE;'
song_table_drop = 'DROP TABLE IF EXISTS dwh.songs CASCADE;'
artist_table_drop = 'DROP TABLE IF EXISTS dwh.artists CASCADE;'
time_table_drop = 'DROP TABLE IF EXISTS dwh.time CASCADE;'

# create staging and dwh schemas

staging_schema = \
    """create schema IF NOT EXISTS staging authorization dwhuser;"""
dwh_schema = \
    """create schema IF NOT EXISTS dwh authorization dwhuser;"""

# CREATE TABLES

staging_songs_table_create = \
    """
CREATE TABLE IF NOT EXISTS songs
             (
                          num_songs        int
                        , artist_id        varchar
                        , artist_latitude  varchar
                        , artist_longitude varchar
                        , artist_location  varchar
                        , artist_name      varchar 
                        , song_id          varchar SORTKEY DISTKEY
                        , title            varchar
                        , duration decimal
                        , year int 
) diststyle key;
"""

staging_events_table_create = \
    """
CREATE TABLE IF NOT EXISTS events
             (
                          artist        varchar
                        , auth          varchar
                        , firstName     varchar
                        , gender        char(1)
                        , iteminSession int SORTKEY
                        , lastName      varchar
                        , length decimal
                        , level    varchar
                        , location varchar
                        , method   char(3)
                        , page     varchar
                        , registration decimal
                        , sessionId int DISTKEY
                        , song      varchar
                        , status    int
                        , start_time bigint
                        , userAgent varchar
                        , userId    int
             ) diststyle Key;
"""

songplay_table_create = \
    """
CREATE TABLE IF NOT EXISTS fact_songplays 
  ( 
     songplay_id        INT PRIMARY KEY NOT NULL IDENTITY(1, 1), 
     starttime         TIMESTAMP REFERENCES dim_time(start_time), 
     user_id            INT REFERENCES dim_users(user_id), 
     level              VARCHAR, 
     song_id            VARCHAR REFERENCES dim_songs(song_id), 
     artist_id          VARCHAR REFERENCES dim_artists(artist_id), 
     session_id         INT, 
     location           VARCHAR, 
     user_agent         VARCHAR, 
     create_timestamp   TIMESTAMP default getdate()
  );
"""

user_table_create = \
    """
CREATE TABLE IF NOT EXISTS dim_users 
  ( 
     user_id            INT  NOT NULL PRIMARY KEY, 
     first_name         VARCHAR, 
     last_name          VARCHAR, 
     gender             CHAR(1), 
     level              VARCHAR NOT NULL, 
     create_timestamp   TIMESTAMP default getdate()
  ) diststyle all;
"""

song_table_create = \
    """
CREATE TABLE IF NOT EXISTS dim_songs 
  ( 
     song_id            VARCHAR NOT NULL PRIMARY KEY, 
     title              VARCHAR, 
     artist_id          VARCHAR, 
     year               INT, 
     duration           NUMERIC, 
     create_timestamp   TIMESTAMP default getdate()
  ) diststyle all;
"""

artist_table_create = \
    """
CREATE TABLE IF NOT EXISTS dim_artists 
  ( 
     artist_id          VARCHAR NOT NULL PRIMARY KEY, 
     name               VARCHAR, 
     location           VARCHAR, 
     latitude           VARCHAR, 
     longitude          VARCHAR, 
     create_timestamp   TIMESTAMP, 
     modified_timestamp TIMESTAMP 
  ) diststyle all;
"""

time_table_create = \
    """
CREATE TABLE IF not EXISTS dim_time 
  ( 
     time_key   bigint NOT NULL, 
     start_time TIMESTAMP NOT NULL UNIQUE, 
     hour       INT, 
     day        INT, 
     week       INT, 
     month      INT, 
     year       INT, 
     weekday    INT, 
     create_timestamp timestamp default getdate(),
     PRIMARY kEY(start_time)
  );
"""

# STAGING TABLES

staging_events_copy = \
    """COPY staging.events FROM {} credentials 'aws_iam_role={}' region 'us-west-2' json {};""".format(config['S3'
        ]['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3'
        ]['LOG_JSONPATH'])

staging_songs_copy = \
    """COPY staging.songs FROM {} credentials 'aws_iam_role={}' region 'us-west-2' json 'auto';""".format(config['S3'
        ]['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = \
    """INSERT INTO dwh.fact_songplays(starttime,user_id, level, song_id,artist_id,session_id,location,
                            user_agent) 
            select TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time, 
             u.user_id, 
             e.level, 
             s.song_id, 
             a.artist_id, 
             e.sessionId, 
             e.location, 
             e.userAgent
             from staging.events as e left outer join dwh.dim_users u on u.user_id = e.userid
             left outer join dwh.dim_songs s on e.song=s.title
             left outer join dwh.dim_artists a on e.artist=a.name and a.location = e.location;
"""

user_table_insert = \
    """INSERT INTO dwh.dim_users(user_id,first_name,last_name,gender,level)
            select distinct
                         userId, 
                         firstName, 
                         lastName, 
                         gender, 
                         level
                         from staging.events where userId is not null;
"""

song_table_insert = \
    """INSERT INTO dwh.dim_songs(song_id,title, artist_id, year, duration)
            select distinct
                        song_id, 
                        title, 
                        artist_id, 
                        year,  
                        duration
                        from staging.songs;
"""

artist_table_insert = \
    """INSERT INTO dwh.dim_artists(artist_id, name, location, latitude, longitude)
            select distinct
                        artist_id, 
                        artist_name, 
                        artist_location, 
                        artist_latitude, 
                        artist_longitude
                        from staging.songs ;
"""

time_table_insert = \
    """INSERT INTO dwh.dim_time(time_key,start_time, hour, day, week, month, year, weekday)
            SELECT distinct
            a.time_key,
            a.start_time,
            EXTRACT (HOUR FROM a.start_time), 
            EXTRACT (DAY FROM a.start_time),
            EXTRACT (WEEK FROM a.start_time), 
            EXTRACT (MONTH FROM a.start_time),
            EXTRACT (YEAR FROM a.start_time), 
            EXTRACT (WEEKDAY FROM a.start_time)
            from
            (SELECT start_time as time_key,TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time from staging.events) a;
"""

user_table_delete = \
    """DELETE FROM dwh.dim_users USING staging.events where dwh.dim_users.user_id = staging.events.userId;
"""
song_table_delete = \
    """DELETE FROM dwh.dim_songs USING staging.songs where dwh.dim_songs.song_id = staging.songs.song_id;
"""
artist_table_delete = \
    """DELETE FROM dwh.dim_artists USING staging.songs where dwh.dim_artists.artist_id = staging.songs.artist_id;
"""
time_table_delete = \
    """DELETE FROM dwh.dim_time USING staging.events where staging.events.start_time =dwh.dim_time.time_key;
"""

# QUERY LISTS

create_schema_queries = [staging_schema, dwh_schema]
create_staging_table_queries = [staging_events_table_create,
                                staging_songs_table_create]
create_dwh_table_queries = [user_table_create, song_table_create,
                            artist_table_create, time_table_create,
                            songplay_table_create]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
delete_table_queries = [user_table_delete, song_table_delete,
                        artist_table_delete, time_table_delete]
insert_table_queries = [user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert,
                        songplay_table_insert]