
-- https://www.postgresql.org/docs/13/ddl-basics.html
-- https://www.postgresql.org/docs/13/ddl-constraints.html (PK,FK)
-- https://www.postgresql.org/docs/13/datatype.html

-- create schema songplay;

CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

-- postgresql+psycopg2://<user>:<password>@<host>/<db>
-- postgresql+psycopg2://airflow_user:airflow_pass@postgresql/airflow_db

-- to use database airflow_db
\c airflow_db