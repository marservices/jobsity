-- COLOCAR OS COMANDOS ABAIXO PARA SEREM EXECUTADOS AO CRIAR O BANCO

CREATE USER 'jobsity' IDENTIFIED BY 'jobsity';

GRANT ALL ON *.* TO 'jobsity';

-- SCRIPT QUE FARÁ PARTE DO PYTHON

CREATE SCHEMA IF NOT EXISTS TRIPS_RAW;

USE TRIPS_RAW;

CREATE TABLE IF NOT EXISTS TRIPS
(
	REGION VARCHAR(50) NOT NULL,
    ORIGIN_COORD VARCHAR(50) NOT NULL,
    DESTINATION_COORD VARCHAR(50) NOT null,
	DATETIME TIMESTAMP NOT NULL,
    DATASOURCE VARCHAR(50) NOT NULL
);

CREATE SCHEMA IF NOT EXISTS TRIPS_DW;

USE TRIPS_DW;

CREATE TABLE IF NOT EXISTS WF_TRIPS
(
	TRIP_ID INT AUTO_INCREMENT NOT NULL,
    REGION_ID INT NOT NULL,
    DATASOURCE_ID INT NOT NULL,
    TRIP_DATETIME TIMESTAMP NOT NULL,
    LAT_ORIGIN FLOAT NOT NULL,
    LON_ORIGIN FLOAT NOT NULL,
    LAT_DEST FLOAT NOT NULL,
    LON_DEST FLOAT NOT NULL,
    CONSTRAINT PK_TRIP PRIMARY KEY (TRIP_ID)
);

CREATE TABLE IF NOT EXISTS WD_REGION
(
	REGION_ID INT AUTO_INCREMENT NOT NULL,
    REGION VARCHAR(50) NOT NULL,
    CONSTRAINT PK_REGION PRIMARY KEY (REGION_ID)
);
    
CREATE TABLE IF NOT EXISTS WD_DATASOURCE
(
	DATASOURCE_ID INT AUTO_INCREMENT NOT NULL,
    DATASOURCE VARCHAR(50) NOT NULL,
    CONSTRAINT PK_DATASOURCE PRIMARY KEY (DATASOURCE_ID)
);