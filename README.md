## Description of the project

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.

In order to do so, Several key topics/steps were achieved:
* Define fact and dimension tables for a star schema (relational database!):
    + The goal is to make analytics on these tables afterwards, to bring business insights for Sparkify
* Write an ETL pipeline that transfers data from files in two local directories (song datas and log datas) into these tables in Postgres using Python and SQL.

## The star Schema

### Fact table:

* songplays:
            + songplay_id serial,
            + start_time timestamp NOT NULL, 
            + user_id int NOT NULL, 
            + level varchar, 
            + song_id varchar, 
            + artist_id varchar, 
            + session_id int, 
            + location varchar, 
            + user_agent varchar, 
            + PRIMARY KEY(songplay_id)

### Dimension tables

* users 
        + user_id int PRIMARY KEY, 
        + first_name varchar, 
        + last_name varchar, 
        + gender varchar, 
        + level varchar
* songs
        + song_id varchar PRIMARY KEY, 
        + title varchar, 
        + artist_id varchar, 
        + year int, 
        + duration float

* artists
        + artist_id varchar PRIMARY KEY,
        + name varchar, 
        + location varchar, 
        + latitude float, 
        + longitude float
* time
        + start_time timestamp PRIMARY KEY,
        + hour varchar, 
        + day varchar, 
        + week varchar, 
        + month varchar, 
        + year varchar, 
        + weekday varchar

## How we handle duplicate rows, in the ETL

We use the ON CONFLICT clause to deal with the problem of insertion, in case of potential duplicates. Two cases:
* We want to update the row with the new insertion:
    + Case of the users table: ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
* We want to ignore the new insertion:
    + Other dimension tables: ON CONFLICT (song_id) DO NOTHING
        
## How to run the script:

In the terminal, type the two following commands:

* python create_tables.py
* python etl.py
