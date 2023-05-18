CREATE ROLE DEVELOPER;-- Creating a new role called Developer
CREATE ROLE ADMIN; -- Creating a new role called Admin
CREATE ROLE PII;-- Creating a new role called PII

-- Making relations between the created roles according to the requirement
GRANT ROLE DEVELOPER TO ROLE ADMIN;
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;
-- Making a Warehouse 
CREATE OR REPLACE WAREHOUSE assignment_wh WITH WAREHOUSE_SIZE = 'MEDIUM';

USE ROLE ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN; -- Granting all privillages to admin 
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin; -- Granting create database privillages to admin to create database                                                        according to requiremnet
CREATE DATABASE assignment_db;-- Database creation
CREATE SCHEMA my_schema;-- Schema creation
use assignment_db; -- db creation
CREATE TABLE EMPLOYEE_DATA (--- Table to store employee data coming from a local csv
  ID NUMBER,
  FIRST_NAME VARCHAR(255),
  LAST_NAME VARCHAR(255),
  EMAIL VARCHAR(255),
  DEPARTMENT VARCHAR(255),
  MOBILE_NUMBER VARCHAR(255),
  CITY VARCHAR(255),
  etl_timestamp timestamp default current_timestamp(), --  time at which the record is getting inserted
  etl_by varchar default 'snowsight',--  application name from which the record was inserted 
  file_name varchar -- the name of the file used to insert data into the table.
);
CREATE OR REPLACE FILE FORMAT my_json_format TYPE = JSON; -- creating a file format to make a stage
CREATE TABLE variant_table (                     
    variant_data variant
);
COPY INTO variant_table FROM @%variant_table file_format = my_json_format; -- Creating a variant table `
select * from variant_table;-- displaying contents in variant table


CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;-- Creating csv file format with a delimiter`
CREATE STAGE internal_stage file_format = my_csv_format; -- Creating internal stage with csv file format

create stage external_stage; -- Creating an external stage
GRANT ALL ON INTEGRATION s3_integration TO ROLE ADMIN;-- Granting all s3 privllages to pull files from an s3 bucket

CREATE STORAGE INTEGRATION s3_integration -- stoarge integrartion object to create the external stage
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::574344495913:role/mysnowflakerole'
storage_allowed_locations = ('s3://snowflakes3bucketassignment/employeeinformation.csv');

DESC INTEGRATION s3_integration;
CREATE OR REPLACE STAGE external_stage -- create external stage with storage integration
URL = 's3://snowflakes3bucketassignment/employeeinformation.csv'
STORAGE_INTEGRATION = s3_integration
FILE_FORMAT = my_csv_format;
LIST @internal_stage;
LIST @external_stage;

Create OR REPLACE TABLE employee_internal LIKE EMPLOYEE_DATA;-- Creating employee_internal table with the same schema as employee data
Create OR REPLACE TABLE employee_external LIKE EMPLOYEE_DATA;-- Creating employee_internal table with the same schema as employee data
COPY INTO employee_internal(id, first_name,last_name,email,department,MOBILE_NUMBER ,city,file_name)-- copying data from internal stage using metadata function and addressing every column using the $ operator 
FROM ( 
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @internal_stage/employeeinformation.csv.gz (file_format => my_csv_format) emp);

COPY INTO employee_external(id, first_name,last_name,email,department,MOBILE_NUMBER ,city,file_name)-- copying data from external stage using metadata function and addressing every column using the $ operator 
FROM (         
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @external_stage (file_format => my_csv_format) emp);

select * from employee_internal limit 10;-- Displaying contents of the copied table from the respective stages

select * from employee_external limit 10;-- Displaying contents of the copied table from the respective stages

CREATE FILE FORMAT my_parquet_format TYPE = parquet;-- Creating a file format with parquet
CREATE STAGE parquet_stage file_format = my_parquet_format;-- Creating a stage with the above created file format

SELECT * -- displaying the table from the parquet stage that was uploaded locally through snowsql
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@parquet_stage'
      , FILE_FORMAT=>'my_parquet_format'
      )
    );


SELECT * from @parquet_stage/employeeinformation.parquet;  

CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string -> -- Creating masking polcies for email and contact number columns
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;

CREATE OR REPLACE MASKING POLICY contact_Mask AS (VAL string) RETURNS string ->
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;

ALTER TABLE IF EXISTS employee_internal MODIFY EMAIL SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee_external MODIFY EMAIL SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee_internal MODIFY mobile_number SET MASKING POLICY contact_mask;
ALTER TABLE IF EXISTS employee_external MODIFY mobile_number SET MASKING POLICY contact_mask;
SELECT * FROM employee_internal LIMIT 10;-- testing various roles to access data and see the masking action
SELECT * FROM employee_external LIMIT 10;
USE ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external TO ROLE PII;
USE ROLE PII;
SELECT * FROM employee_internal LIMIT 10;
SELECT * FROM employee_external LIMIT 10;

USE ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external TO ROLE DEVELOPER;
USE ROLE DEVELOPER;

SELECT * FROM employee_internal LIMIT 10; 
SELECT * FROM employee_internal LIMIT 10; 