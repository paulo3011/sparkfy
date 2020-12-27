# Project: Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Questions to anwser

- What songs users are listening to? (Currently, they don't have an easy way to query their data)

# Checklist

- [ok] Create a database schema with fact and dimension tables for a star schema
- [ok] Create a Postgres database with tables designed to optimize queries on song play analysis
- [ok] Create an ETL pipeline that transfers data from files in __two local directories__ into these tables in Postgres using Python and SQL
- [ok] You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify
- [-] Compare your results with their expected results


# Requirements

## Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables

users - users in the app
- user_id, first_name, last_name, gender, level

songs - songs in music database
- song_id, title, artist_id, year, duration

artists - artists in music database
- artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

# Results and How to run

## [To test using docker-compose do](#dockercompose):

Clone the git repository

```shell
cd ~/myprojectfolder/
git clone https://github.com/paulo3011/sparkfy.git
cd sparkfy
````
Run docker-compose

```shell
cd project1/docker
docker-compose up
```
The execution above should show some like

```shell
Creating network "sparkify_prj01_sparkify" with the default driver
Creating sparkify-pgsql   ... done
Creating sparkify-adminer ... done
Creating sparkify-etl     ... done
Attaching to sparkify-pgsql, sparkify-adminer, sparkify-etl
sparkify-adminer | [Sun Dec 27 20:15:53 2020] PHP 7.4.13 Development Server (http://[::]:8080) started
sparkify-pgsql | The files belonging to this database system will be owned by user "postgres".
sparkify-pgsql | This user must also own the server process.
sparkify-pgsql |
sparkify-pgsql | The database cluster will be initialized with locale "en_US.utf8".
sparkify-pgsql | The default database encoding has accordingly been set to "UTF8".
sparkify-pgsql | The default text search configuration will be set to "english".
sparkify-pgsql |
sparkify-pgsql | Data page checksums are disabled.
sparkify-pgsql |
sparkify-pgsql | fixing permissions on existing directory /var/lib/postgresql/data ... ok
sparkify-pgsql | creating subdirectories ... ok
sparkify-pgsql | selecting dynamic shared memory implementation ... posix
sparkify-pgsql | selecting default max_connections ... 100
sparkify-pgsql | selecting default shared_buffers ... 128MB
sparkify-pgsql | selecting default time zone ... Etc/UTC
sparkify-pgsql | creating configuration files ... ok
sparkify-pgsql | running bootstrap script ... ok
sparkify-pgsql | performing post-bootstrap initialization ... ok
sparkify-pgsql | syncing data to disk ... ok
sparkify-pgsql |
sparkify-pgsql | initdb: warning: enabling "trust" authentication for local connections
sparkify-pgsql | You can change this by editing pg_hba.conf or using the option -A, or
sparkify-pgsql | --auth-local and --auth-host, the next time you run initdb.
sparkify-pgsql |
sparkify-pgsql | Success. You can now start the database server using:
sparkify-pgsql |
sparkify-pgsql |     pg_ctl -D /var/lib/postgresql/data -l logfile start
sparkify-pgsql |
sparkify-pgsql | waiting for server to start....2020-12-27 20:15:54.484 UTC [46] LOG:  starting PostgreSQL 13.1 (Debian 13.1-1.pgdg100+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 8.3.0-6) 8.3.0, 64-bit
sparkify-pgsql | 2020-12-27 20:15:54.486 UTC [46] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
sparkify-pgsql | 2020-12-27 20:15:54.493 UTC [47] LOG:  database system was shut down at 2020-12-27 20:15:54 UTC
sparkify-pgsql | 2020-12-27 20:15:54.498 UTC [46] LOG:  database system is ready to accept connections
sparkify-pgsql |  done
sparkify-pgsql | server started
sparkify-pgsql | CREATE DATABASE
sparkify-pgsql |
sparkify-pgsql |
sparkify-pgsql | /usr/local/bin/docker-entrypoint.sh: running /docker-entrypoint-initdb.d/01.sql
sparkify-pgsql | DROP DATABASE
sparkify-pgsql | psql:/docker-entrypoint-initdb.d/01.sql:11: NOTICE:  database "sparkifydb" does not exist, skipping
sparkify-pgsql | CREATE DATABASE
sparkify-pgsql | You are now connected to database "sparkifydb" as user "student".
sparkify-pgsql | CREATE TABLE
sparkify-pgsql | CREATE TABLE
sparkify-pgsql | CREATE TABLE
sparkify-pgsql | CREATE TABLE
sparkify-pgsql | CREATE TABLE
sparkify-pgsql | COMMENT
sparkify-pgsql |
sparkify-pgsql |
sparkify-pgsql | waiting for server to shut down...2020-12-27 20:15:54.965 UTC [46] LOG:  received fast shutdown request
sparkify-pgsql | .2020-12-27 20:15:54.967 UTC [46] LOG:  aborting any active transactions
sparkify-pgsql | 2020-12-27 20:15:54.968 UTC [46] LOG:  background worker "logical replication launcher" (PID 53) exited with exit code 1
sparkify-pgsql | 2020-12-27 20:15:54.968 UTC [48] LOG:  shutting down
sparkify-pgsql | 2020-12-27 20:15:54.994 UTC [46] LOG:  database system is shut down
sparkify-pgsql |  done
sparkify-pgsql | server stopped
sparkify-pgsql |
sparkify-pgsql | PostgreSQL init process complete; ready for start up.
sparkify-pgsql |
sparkify-pgsql | 2020-12-27 20:15:55.087 UTC [1] LOG:  starting PostgreSQL 13.1 (Debian 13.1-1.pgdg100+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 8.3.0-6) 8.3.0, 64-bit
sparkify-pgsql | 2020-12-27 20:15:55.087 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
sparkify-pgsql | 2020-12-27 20:15:55.087 UTC [1] LOG:  listening on IPv6 address "::", port 5432
sparkify-pgsql | 2020-12-27 20:15:55.092 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
sparkify-pgsql | 2020-12-27 20:15:55.101 UTC [74] LOG:  database system was shut down at 2020-12-27 20:15:54 UTC
sparkify-pgsql | 2020-12-27 20:15:55.107 UTC [1] LOG:  database system is ready to accept connections
sparkify-etl | running action =>  runjob
sparkify-etl | starting ETL
sparkify-etl | got error could not connect to server: Connection refused
sparkify-etl |  Is the server running on host "postgres" (192.168.240.3) and accepting
sparkify-etl |  TCP/IP connections on port 5432?. reconnecting 1
sparkify-etl | 71 files found in data/song_data
sparkify-etl | 1/71 files processed.
sparkify-etl | 2/71 files processed.
sparkify-etl | 3/71 files processed.
sparkify-etl | 4/71 files processed.
sparkify-etl | 5/71 files processed.
sparkify-etl | 6/71 files processed.
sparkify-etl | 7/71 files processed.
sparkify-etl | 8/71 files processed.
sparkify-etl | 9/71 files processed.
sparkify-etl | 10/71 files processed.
sparkify-etl | 11/71 files processed.
sparkify-etl | 12/71 files processed.
sparkify-etl | 13/71 files processed.
sparkify-etl | 14/71 files processed.
sparkify-etl | 15/71 files processed.
sparkify-etl | 16/71 files processed.
sparkify-etl | 17/71 files processed.
sparkify-etl | 18/71 files processed.
sparkify-etl | 19/71 files processed.
sparkify-etl | 20/71 files processed.
sparkify-etl | 21/71 files processed.
sparkify-etl | 22/71 files processed.
sparkify-etl | 23/71 files processed.
sparkify-etl | 24/71 files processed.
sparkify-etl | 25/71 files processed.
sparkify-etl | 26/71 files processed.
sparkify-etl | 27/71 files processed.
sparkify-etl | 28/71 files processed.
sparkify-etl | 29/71 files processed.
sparkify-etl | 30/71 files processed.
sparkify-etl | 31/71 files processed.
sparkify-etl | 32/71 files processed.
sparkify-etl | 33/71 files processed.
sparkify-etl | 34/71 files processed.
sparkify-etl | 35/71 files processed.
sparkify-etl | 36/71 files processed.
sparkify-etl | 37/71 files processed.
sparkify-etl | 38/71 files processed.
sparkify-etl | 39/71 files processed.
sparkify-etl | 40/71 files processed.
sparkify-etl | 41/71 files processed.
sparkify-etl | 42/71 files processed.
sparkify-etl | 43/71 files processed.
sparkify-etl | 44/71 files processed.
sparkify-etl | 45/71 files processed.
sparkify-etl | 46/71 files processed.
sparkify-etl | 47/71 files processed.
sparkify-etl | 48/71 files processed.
sparkify-etl | 49/71 files processed.
sparkify-etl | 50/71 files processed.
sparkify-etl | 51/71 files processed.
sparkify-etl | 52/71 files processed.
sparkify-etl | 53/71 files processed.
sparkify-etl | 54/71 files processed.
sparkify-etl | 55/71 files processed.
sparkify-etl | 56/71 files processed.
sparkify-etl | 57/71 files processed.
sparkify-etl | 58/71 files processed.
sparkify-etl | 59/71 files processed.
sparkify-etl | 60/71 files processed.
sparkify-etl | 61/71 files processed.
sparkify-etl | 62/71 files processed.
sparkify-etl | 63/71 files processed.
sparkify-etl | 64/71 files processed.
sparkify-etl | 65/71 files processed.
sparkify-etl | 66/71 files processed.
sparkify-etl | 67/71 files processed.
sparkify-etl | 68/71 files processed.
sparkify-etl | 69/71 files processed.
sparkify-etl | 70/71 files processed.
sparkify-etl | 71/71 files processed.
sparkify-etl | 30 files found in data/log_data
sparkify-etl | 1/30 files processed.
sparkify-etl | 2/30 files processed.
sparkify-etl | 3/30 files processed.
sparkify-etl | 4/30 files processed.
sparkify-etl | 5/30 files processed.
sparkify-etl | 6/30 files processed.
sparkify-etl | 7/30 files processed.
sparkify-etl | 8/30 files processed.
sparkify-etl | 9/30 files processed.
sparkify-etl | 10/30 files processed.
sparkify-etl | 11/30 files processed.
sparkify-etl | 12/30 files processed.
sparkify-etl | 13/30 files processed.
sparkify-etl | 14/30 files processed.
sparkify-etl | 15/30 files processed.
sparkify-etl | 16/30 files processed.
sparkify-etl | 17/30 files processed.
sparkify-etl | 18/30 files processed.
sparkify-etl | 19/30 files processed.
sparkify-etl | 20/30 files processed.
sparkify-etl | 21/30 files processed.
sparkify-etl | 22/30 files processed.
sparkify-etl | 23/30 files processed.
sparkify-etl | 24/30 files processed.
sparkify-etl | 25/30 files processed.
sparkify-etl | 26/30 files processed.
sparkify-etl | 27/30 files processed.
sparkify-etl | 28/30 files processed.
sparkify-etl | 29/30 files processed.
sparkify-etl | 30/30 files processed.
sparkify-etl | [(6820, 'songplays'), (1, 'songplays_with_song_id'), (69, 'artists'), (71, 'artists'), (96, 'users'), (6813, 'time')]
sparkify-etl exited with code 0
```

## To test execution in the same way that udacity's jupyter does:

Note: the database needs to be up, you can use [docker-compose](#dockercompose) file to do this.

```shell
cd project1/runtime/
python jupyter_test.py
starting etl test
71 files found in /home/paulo/projects/paulo3011/sparkfy/project1/runtime/data/song_data
1/71 files processed.
2/71 files processed.
3/71 files processed.
4/71 files processed.
5/71 files processed.
6/71 files processed.
7/71 files processed.
8/71 files processed.
9/71 files processed.
10/71 files processed.
11/71 files processed.
12/71 files processed.
13/71 files processed.
14/71 files processed.
15/71 files processed.
16/71 files processed.
17/71 files processed.
18/71 files processed.
19/71 files processed.
20/71 files processed.
21/71 files processed.
22/71 files processed.
23/71 files processed.
24/71 files processed.
25/71 files processed.
26/71 files processed.
27/71 files processed.
28/71 files processed.
29/71 files processed.
30/71 files processed.
31/71 files processed.
32/71 files processed.
33/71 files processed.
34/71 files processed.
35/71 files processed.
36/71 files processed.
37/71 files processed.
38/71 files processed.
39/71 files processed.
40/71 files processed.
41/71 files processed.
42/71 files processed.
43/71 files processed.
44/71 files processed.
45/71 files processed.
46/71 files processed.
47/71 files processed.
48/71 files processed.
49/71 files processed.
50/71 files processed.
51/71 files processed.
52/71 files processed.
53/71 files processed.
54/71 files processed.
55/71 files processed.
56/71 files processed.
57/71 files processed.
58/71 files processed.
59/71 files processed.
60/71 files processed.
61/71 files processed.
62/71 files processed.
63/71 files processed.
64/71 files processed.
65/71 files processed.
66/71 files processed.
67/71 files processed.
68/71 files processed.
69/71 files processed.
70/71 files processed.
71/71 files processed.
30 files found in /home/paulo/projects/paulo3011/sparkfy/project1/runtime/data/log_data
1/30 files processed.
2/30 files processed.
3/30 files processed.
4/30 files processed.
5/30 files processed.
6/30 files processed.
7/30 files processed.
8/30 files processed.
9/30 files processed.
10/30 files processed.
11/30 files processed.
12/30 files processed.
13/30 files processed.
14/30 files processed.
15/30 files processed.
16/30 files processed.
17/30 files processed.
18/30 files processed.
19/30 files processed.
20/30 files processed.
21/30 files processed.
22/30 files processed.
23/30 files processed.
24/30 files processed.
25/30 files processed.
26/30 files processed.
27/30 files processed.
28/30 files processed.
29/30 files processed.
30/30 files processed.
[(6820, 'songplays'), (1, 'songplays_with_song_id'), (69, 'artists'), (71, 'artists'), (96, 'users'), (6813, 'time')]
```
