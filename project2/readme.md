# Project: Data Modeling with Cassandra

## Project Description



# Checklist

- []


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
cd project2/docker
docker-compose up
```
The execution above should show some like

```shell

```

## To test execution in the same way that udacity's jupyter does:

Note: the database needs to be up, you can use [docker-compose](#dockercompose) file to do this.

```shell
cd project2/runtime/
python jupyter_test.py

```

To reset docker-compose run

```shell
cd project2/docker
# this will drop everthing
docker-compose down
```

# Project files

## project2/runtime

Auxiliary pythons and ETL routine files.

## project2/docker

A different way to run, develop and test ETL using docker and docker-compose.

The database in this form is initialized by the script: project2/docker/initdb/postgres/01.sql
