-- Runs once, when the Postgres volume is first created.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER metabase WITH PASSWORD 'metabase';
CREATE DATABASE metabase OWNER metabase;

-- The serving warehouse your gold jobs load into
CREATE USER ott WITH PASSWORD 'ott';
CREATE DATABASE ottdw OWNER ott;
