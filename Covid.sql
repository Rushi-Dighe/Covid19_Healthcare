--1. Create database covid_db and schema schema

create or replace role project_role;

GRANT role PROJECT_ROLE to role SYSADMIN;

create or replace database covid_db;

create or replace schema covid_schema;



CREATE OR REPLACE USER nandini PASSWORD = 'project@123';
CREATE OR REPLACE USER rushi PASSWORD = 'project@123';
CREATE OR REPLACE USER rohan_sharma PASSWORD = 'project@123';
CREATE OR REPLACE USER lalit PASSWORD = 'project@123';

CREATE OR REPLACE WAREHOUSE COVID_WH;

GRANT USAGE ON WAREHOUSE COVID_WH TO ROLE PROJECT_ROLE;

GRANT ALL ON WAREHOUSE COVID_WH TO ROLE PROJECT_ROLE;

grant all on schema covid_schema to role projct_role;

USE ROLE SECURITYADMIN;
GRANT ROLE project_role TO USER lalit;
GRANT ROLE project_role TO USER nandini;
GRANT ROLE project_role TO USER Rushi;
GRANT ROLE project_role TO USER rohan_sharma;



--2. Create a covid_history table to load all the historical datasets and apply cluster
--key on the most used column for querying.
--3. Add the table definition with constraints , unicode and data retention.

   
create or replace TABLE COVID_DB.COVID_SCHEMA.COVID_HISTORY (
	PROVINCE STRING,
	COUNTRY STRING NOT NULL,
	LASTUPDATE STRING,
	CONFIRMED VARCHAR(5),
	DEATHS NUMBER(38,0),
	RECOVERED NUMBER(38,0)
	
) cluster by (country)
data_retention_time_in_days=1
enable_schema_evolution=true;

desc  table covid_history;



-- create or replace table covid19_history clone covid_history 
-- before(statement=>'01af3ebf-3200-e1f1-0000-00066d6a95cd');



--4. Create internal stage and load every 3 months data which has different schema
--5. Perform basic transformation (cast, change date format) during data load

CREATE OR REPLACE STAGE covid_int_stage;

CREATE OR REPLACE FILE FORMAT covid_ff 
TYPE = 'CSV'
COMPRESSION = 'AUTO' 
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO' 
NULL_IF = ('\\N');

copy into covid_history 
from @covid_int_stage
file_format='covid_ff'
on_error='skip_file';

list @covid_int_stage;

select * from covid19_history;

COPY INTO "COVID_DB"."COVID_SCHEMA"."COVID_HISTORY"
FROM '@"COVID_DB"."COVID_SCHEMA"."%COVID_HISTORY"/__snowflake_temp_import_files__/'
FILES = ('01-22-2020.csv','01-23-2020.csv','01-24-2020.csv','01-25-2020.csv','01-26-2020.csv','01-27-2020.csv','01-28-2020.csv','01-29-2020.csv','01-30-2020.csv','01-31-2020.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    ERROR_ON_COLUMN_COUNT_MISMATCH =  FALSE

)
ON_ERROR='SKIP_FILE'
PURGE=TRUE
validation_mode=return_all_errors

--6. Upload all current data into AWS s3 bucket snow_extstage and perform load

create or replace table covid19_ext2 (
    PROVINCE STRING,
	COUNTRY STRING NOT NULL,
	LASTUPDATE STRING,
	CONFIRMED VARCHAR(10),
	DEATHS NUMBER,
	RECOVERED NUMBER
);

--7. Load the data from external stage

CREATE OR REPLACE STAGE PROJECT_STAGE;

copy into covid19_ext2 
from @project_stage
file_format=covid_ff
on_error='skip_file';


   create or replace storage integration covid_int
   type = external_stage
   storage_provider = s3
   enabled = true
   storage_aws_role_arn = 'arn:aws:iam::151802504720:role/keen_role'
   storage_allowed_locations = ('s3://keenproject01/');

  desc integration covid_int;




list @project_stage;

--drop table covid19_ext2_cdc;

--CREATE OR REPLACE stream covid19_ext2_cdc 
    -- PROVINCE STRING,
    -- COUNTRY STRING NOT NULL,
    -- LASTUPDATE STRING,
    -- CONFIRMED VARCHAR(10),
    -- DEATHS NUMBER,
    -- RECOVERED NUMBER);
    -- valid_from TIMESTAMP_LTZ,
    -- valid_to TIMESTAMP_LTZ,
    -- operation STRING
     --covid19_
     
use role accountadmin;
grant execute task on account to role project_role;

----------SNOWPIPE---------------
---------------------------------

-- STEP 1
create or replace table project_ingest( --staging table
    Province_State string,
    Country_Region string,
    Last_Update string,
    Lat number(38,5),
    Long_ number(38,5),
    Confirmed number,
    Deaths number,
    Recovered number,
    Active STRING,
    FIPS number(38,2),
    Incident_Rate STRING,
    People_Tested number(38,1),
    People_Hospitalized number,
    Mortality_Rate NUMBER(38,10),
    UID number(38,1),
    ISO3 string,
    Testing_Rate number(38,5),
    Hospitalization_Rate number(38,5)
);
-- alter table project_ingest modify recovered varchar(10);
-- alter table project_ingest modify people_hospitalized varchar(10);


-- STEP 2
-- creating pipe stage
create or replace stage pipe_stage
URL ='s3://demo21092023/us_data/'
CREDENTIALS = (AWS_KEY_ID='AKIA44IFKXANE7IBTAE7'
               AWS_SECRET_KEY='VNi2x61PrycgLuRxC3h+5OGTcOpVxm3UZ5ux3cLY');


LIST @PIPE_STAGE;

-- STEP3
-- CREATE PIPE
CREATE OR REPLACE PIPE COVID_PIPE
AUTO_INGEST = TRUE
AS COPY INTO PROJECT_INGEST
FROM @PIPE_STAGE
FILE_FORMAT = 'COVID_FF';

SHOW PIPES;

SELECT * FROM PROJECT_INGEST;

alter pipe covid_pipe refresh;
-----------------------------------

--------------------
select * from table(validate_pipe_load(
  pipe_name=>'COVID_PIPE',
  start_time=>dateadd(hour, -1, current_timestamp())));


-------------- FOR VALIDATING PIPE------------
select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>to_timestamp_tz('2023-09-27 12:00:00.000 -0700'),
    date_range_end=>to_timestamp_tz('2023-09-27 16:30:00.000 -0700')));


---- STREAM 
CREATE OR REPLACE STREAM COVID_STRM
ON TABLE PROJECT_INGEST
APPEND_ONLY = FALSE;

SELECT * FROM COVID_STRM;

CREATE OR REPLACE TABLE CONSUMPTION_TABLE LIKE PROJECT_INGEST;
SELECT * FROM CONSUMPTION_TABLE;

----TASK 
CREATE OR REPLACE TASK INGEST_TASK
WAREHOUSE= COVID_WH
SCHEDULE='5 MINUTE'
WHEN 
SYSTEM$STREAM_HAS_DATA('COVID_STRM')
AS
INSERT INTO CONSUMPTION_TABLE
SELECT Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,
Deaths,Recovered,Active,FIPS,Incident_Rate,People_Tested,People_Hospitalized,
Mortality_Rate,UID,ISO3,Testing_Rate,Hospitalization_Rate FROM COVID_STRM;

SHOW TASKS;

ALTER TASK INGEST_TASK RESUME;


-------TASK 11
CREATE OR REPLACE TABLE FINAL_TABLE CLONE CONSUMPTION_TABLE AT(OFFSET => -60*60*24);

SELECT * FROM CONSUMPTION_TABLE AT(TIMESTAMP => 'Fri, 30 September 2023 16:20:00 -0700');

SELECT * FROM FINAL_TABLE;

---task 12
---------
-- CREATE PROCEDURE InsertDataWithDateCast
--     @in_id INT,
--     @in_date_string VARCHAR(255),
--     @in_other_data VARCHAR(255)
-- AS
-- BEGIN
--     DECLARE @formatted_date DATE;
--     BEGIN TRY
--         SET @formatted_date = CONVERT(DATE, @in_date_string, 120); -- Assuming the date format is YYYY-MM-DD

--         INSERT INTO YourTableName (id, date_column, other_column)
--         VALUES (@in_id, @formatted_date, @in_other_data);

--         PRINT 'Data inserted successfully.';
--     END TRY
--     BEGIN CATCH
--         PRINT
-- EXEC InsertDataWithDateCast 1, '2023-09-27', 'Some Data';

-- 12. Create stored procedure to insert the data into table after typecasting date
-- format


create or replace table sample_table like final_table;
----
-- create or replace procedure cov_procd
-- DECLARE formatted_date DATE;
-- BEGIN
-- select convert(varchar, getdate(), 120)

Create procedure new_proc
-- (@LAST_UPDATE string)
AS
BEGIN
INSERT INTO sample_table
(Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,FIPS,Incident_Rate,People_Tested,People_Hospitalized,Mortality_Rate,UID,ISO3,Testing_Rate,Hospitalization_Rate) --- Columns of smaller table
Values (SELECT Province_State,Country_Region,CONVERT(varchar,@Last_Update,120) as [YYYY-MM-DD HH:MM:SS],Lat,Long_,Confirmed,Deaths,Recovered,Active,FIPS,Incident_Rate,People_Tested,People_Hospitalized,Mortality_Rate,UID,ISO3,Testing_Rate,Hospitalization_Rate ---Columns of Master table
FROM final_table)
-- WHERE last_update = @LAST_UPDATE --- Where value of C1 of Master table matches the value of @C1
END
-- CONVERT(varchar,@Existingdate,120) as [YYYY-MM-DD HH:MM:SS]
























  


   




