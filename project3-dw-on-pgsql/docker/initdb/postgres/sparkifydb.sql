
-- https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/a7801de4-ee3f-4531-b887-82dea67f47a6/lessons/01995bb4-db30-4e01-bf38-ff11b631be0f/concepts/1533c19b-0505-49fd-b1b7-06c987641f0d

-- https://www.postgresql.org/docs/13/ddl-basics.html
-- https://www.postgresql.org/docs/13/ddl-constraints.html (PK,FK)
-- https://www.postgresql.org/docs/13/datatype.html


-- drop to reset the database if exists
drop database if exists sparkifydb;

-- https://www.postgresql.org/docs/13/manage-ag-templatedbs.html
-- CREATE DATABASE actually works by copying an existing database. By default, it copies the standard system database named template1.
-- By instructing CREATE DATABASE to copy template0 instead of template1, you can create a “pristine” (original) user database (one where no user-defined objects exist and where the system objects have not been altered) that contains none of the site-local additions in template1.
create database sparkifydb with encoding 'utf8' TEMPLATE template0;

-- to use database sparkifydb
\c sparkifydb

-- dimension tables
