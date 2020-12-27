
-- https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/a7801de4-ee3f-4531-b887-82dea67f47a6/lessons/01995bb4-db30-4e01-bf38-ff11b631be0f/concepts/1533c19b-0505-49fd-b1b7-06c987641f0d

-- https://www.postgresql.org/docs/13/ddl-basics.html
-- https://www.postgresql.org/docs/13/ddl-constraints.html (PK,FK)
-- https://www.postgresql.org/docs/13/datatype.html

-- create schema songplay;

-- drop to reset the database if exists
drop database if exists sparkifydb;

-- https://www.postgresql.org/docs/13/manage-ag-templatedbs.html
-- CREATE DATABASE actually works by copying an existing database. By default, it copies the standard system database named template1.
-- By instructing CREATE DATABASE to copy template0 instead of template1, you can create a “pristine” user database (one where no user-defined objects exist and where the system objects have not been altered) that contains none of the site-local additions in template1.
create database sparkifydb with encoding 'utf8' TEMPLATE template0;

-- to use database sparkifydb
\c sparkifydb

-- dimension tables

-- users - users in the app
-- https://www.postgresql.org/docs/13/datatype-character.html
-- in fact character(n) is usually the slowest of the three because of its additional storage costs. In most situations text or character varying should be used instead.
create table users
(
    user_id integer -- can have up to 2.147.483.647 users
    ,first_name varchar(60)
    ,last_name varchar(60)
    ,gender varchar(1)
    ,level varchar(4) -- paid or free
    ,PRIMARY KEY (user_id)
);

-- songs - songs in music database
-- https://www.postgresql.org/docs/13/datatype-numeric.html
-- The type numeric can store numbers with a very large number of digits. It is especially recommended for storing monetary amounts and other quantities where exactness is required. Calculations with numeric values yield exact results where possible, e.g., addition, subtraction, multiplication. However, calculations on numeric values are very slow compared to the integer types, or to the floating-point types
-- Floating-point types (real, double) ar inexact. Inexact means that some values cannot be converted exactly to the internal format and are stored as approximations, so that storing and retrieving a value might show slight discrepancies
create table songs
(
    song_id varchar(30)
    ,title varchar(60)
    ,artist_id varchar(30)
    ,year smallint
    ,duration numeric(9,6) -- ex: 218.93179
    ,PRIMARY KEY (song_id)
);

-- artists - artists in music database
-- https://docs.mapbox.com/help/glossary/lat-lon/
-- https://stackoverflow.com/questions/15965166/what-is-the-maximum-length-of-latitude-and-longitude
-- https://en.wikipedia.org/wiki/Geographic_coordinate_system
-- The precision of a numeric is the total count of significant digits in the whole number, that is, the number of digits to both sides of the decimal point. The number 23.5141 has a precision of 6 and a scale of 4. Integers can be considered to have a scale of zero.
create table artists
(
    artist_id varchar(30)
    ,name varchar(160) -- note: "artist_name": "Montserrat Caball\u00e9;Placido Domingo;Vicente Sardinero;Judith Blegen;Sherrill Milnes;Georg Solti"
    ,location varchar(160)
    ,latitude numeric(9,6)  -- ex: 35.14968
    ,longitude numeric(9,6) -- ex: -90.04892
    ,PRIMARY KEY (artist_id)
);

-- time - timestamps of records in songplays broken down into specific units
create table time
(
    start_time bigint not null -- represent the timestamp where one song was played
    ,hour smallint not null
    ,day smallint not null
    ,week smallint not null
    ,month smallint not null
    ,year smallint not null
    ,weekday smallint not null
    ,PRIMARY KEY (start_time)
);

-- fact tables

create table songplays
(
    songplay_id bigserial NOT NULL -- bigserial == default nextval('songplays_id')
    ,start_time bigint
    ,user_id integer        REFERENCES users (user_id)
    ,level varchar(4)       -- paid or free
    ,song_id varchar(30)    NULL  REFERENCES songs (song_id)
    ,artist_id varchar(30)  NULL REFERENCES artists (artist_id)
    ,session_id integer
    ,location varchar(60)
    ,user_agent varchar(400)
    ,PRIMARY KEY (songplay_id)
);

-- COMMENT ON TABLE
COMMENT ON COLUMN songplays.start_time IS 'When the song was played';