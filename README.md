# Snow_Flake_Assignment
## 1) Create roles as per the below-mentioned hierarchy. Accountadmin already exists in Snowflake
<img width="184" alt="Screenshot 2023-05-18 at 4 02 51 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/6f3408f3-1ec5-4e87-8bce-35ce7bec3728">

```
CREATE ROLE DEVELOPER;-- Creating a new role called Developer
CREATE ROLE ADMIN; -- Creating a new role called Admin
CREATE ROLE PII;-- Creating a new role called PII

```
Making relations between the created roles according to the requirement mentioned above

```
GRANT ROLE DEVELOPER TO ROLE ADMIN;
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;
```
## 2) Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries.

```
CREATE OR REPLACE WAREHOUSE assignment_wh WITH WAREHOUSE_SIZE = 'MEDIUM';
```
## 3) Switch to the admin role 

Have to grant privillages to admin role before we can use it.
```
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;
USE ROLE ADMIN;
```
## 4) Create a database assignment_db

```
CREATE DATABASE assignment_db;

```

## 5) Create a schema my_schema 

```
CREATE SCHEMA my_schema;
```

## 6) Create a table using any sample csv. You can get 1 by googling for sample csv's. Preferably search for a sample employee dataset so that you have Pll related columns else you can consider any column as PII 

Used a csv generator to generate a sample csv file from https://extendsclass.com/csv-generator.html

like below
<img width="990" alt="Screenshot 2023-05-18 at 5 29 10 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/4c975a28-6e18-4637-95f7-d327379a2bd6">

```
CREATE TABLE EMPLOYEE_DATA (
  ID NUMBER,
  FIRST_NAME VARCHAR(255),
  LAST_NAME VARCHAR(255),
  EMAIL VARCHAR(255),
  DEPARTMENT VARCHAR(255),
  MOBILE_NUMBER VARCHAR(255),
  CITY VARCHAR(255),
  etl_timestamp timestamp default current_timestamp(), -- for getting the time at which the record is getting inserted
  etl_by varchar default 'snowsight',-- for getting application name from which the record was inserted 
  file_name varchar -- for getting the name of the file used to insert data into the table.
);
```
## 7) Also, create a variant version of this dataset 

```
CREATE OR REPLACE FILE FORMAT my_json_format TYPE = JSON;
CREATE TABLE variant_table (                     
    variant_data variant
);

```
Pushed data on variant table locally using snowsql


<img width="1112" alt="Screenshot 2023-05-18 at 5 57 48 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/07a63869-da7e-4cb3-9eed-86465241be18">

```
COPY INTO variant_table  FROM @%variant_table file_format = my_json_format; 
select * from variant_table;
```

<img width="1118" alt="Screenshot 2023-05-18 at 5 58 42 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/f4ea2076-3041-448a-9f6c-837386d4454c">
## 8) Load the file into an external and internal stage
Create file format and internalstage
```
CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;
CREATE STAGE internal_stage file_format = my_csv_format;
```
Upload to internal stage

<img width="1136" alt="Screenshot 2023-05-18 at 6 11 57 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/f663363b-5c2f-4544-8714-204aa9279152">

Create stage external_stage;
```
create stage external_stage;
```
Granting S3 bucket privllages on snowflake
```
GRANT ALL ON INTEGRATION s3_integration TO ROLE ADMIN;
```
<img width="1084" alt="Screenshot 2023-05-18 at 6 06 28 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/6163326f-7081-4520-a1b8-94f90df6f14f">
<img width="1440" alt="Screenshot 2023-05-18 at 6 09 23 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/aac2e1da-baed-448e-8c9e-c8f067a3a418">
Create S3 integration object
```
CREATE STORAGE INTEGRATION s3_integration --
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::574344495913:role/mysnowflakerole'
storage_allowed_locations = ('s3://snowflakes3bucketassignment/employeeinformation.csv');
```
<img width="1116" alt="Screenshot 2023-05-18 at 6 13 37 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/98270374-62ec-47d8-a5cb-bea3d75656bf">

Create an external stage with s3 integration storage
```
CREATE OR REPLACE STAGE external_stage
URL = 's3://snowflakes3bucketassignment/employeeinformation.csv'
STORAGE_INTEGRATION = s3_integration
FILE_FORMAT = my_csv_format;
```
Internal Stage
<img width="826" alt="Screenshot 2023-05-18 at 6 16 14 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/81a7ece9-5dd1-4fc6-ae05-1dd649500090">
External Stage
<img width="847" alt="Screenshot 2023-05-18 at 6 16 35 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/aefa0aee-cceb-40f9-8244-e2a5dd3abb53">

## 9) Load data into the tables using copy into statements. In one table load from the internal stage and in another from the external

Create internal and external tables 
```
Create OR REPLACE TABLE employee_internal LIKE EMPLOYEE_DATA;
Create OR REPLACE TABLE employee_external LIKE EMPLOYEE_DATA;
```
Using the meta data function we copy the data in the respective stages to employee_internal and employee_external table
```
COPY INTO employee_internal(id, first_name,last_name,email,department,MOBILE_NUMBER ,city,file_name)
FROM ( 
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @internal_stage/employeeinformation.csv.gz (file_format => my_csv_format) emp);

COPY INTO employee_external(id, first_name,last_name,email,department,MOBILE_NUMBER ,city,file_name)
FROM (         
SELECT emp.$1, emp.$2, emp.$3, emp.$4, emp.$5, emp.$6, emp.$7, METADATA$FILENAME 
FROM @external_stage (file_format => my_csv_format) emp);
```
Displaying the table information :
```
select * from employee_internal limit 10;

```
<img width="1114" alt="Screenshot 2023-05-18 at 6 22 51 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/14a1a358-6d21-49a0-8001-251098895d8f">


```
select * from employee_external limit 10;
```
<img width="1121" alt="Screenshot 2023-05-18 at 6 23 16 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/1097c3ba-6828-40a9-8ab6-fee417c9bf81">

## 10. Upload any parquet file to the stage location and infer the schema of the file 

Create file format for parquet
```

CREATE FILE FORMAT my_parquet_format TYPE = parquet;
CREATE STAGE parquet_stage file_format = my_parquet_format;
```
Uploading locally through snowsql
<img width="1436" alt="Screenshot 2023-05-18 at 6 29 33 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/07023094-eedb-417f-9b89-370436b7d56d">

Displaying the Table
```
SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@parquet_stage'
      , FILE_FORMAT=>'my_parquet_format'
      )
    );
```
<img width="1121" alt="Screenshot 2023-05-18 at 6 27 57 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/879d59d4-b04f-4bbb-82a7-ca432b7463f4">

# 11. Run a select query on the staged parquet file without loading it to a snowflake table

```
SELECT * from @parquet_stage/employeeinformation.parquet; 
```
<img width="1118" alt="Screenshot 2023-05-18 at 6 31 07 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/313eef4a-5eed-4f09-8798-b0aa14503a55">

# 12.Add masking policy to the Pil columns such that fields like email,phone number, etc. show as "masked" to a user with the developer role If the role is PII the value of these columns should be visible

Make a masking policy on email and contact information and display "****MASK****" when role is not PII
```
CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string ->
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;

CREATE OR REPLACE MASKING POLICY contact_Mask AS (VAL string) RETURNS string ->
CASE
WHEN CURRENT_ROLE() = 'PII' THEN VAL
ELSE '****MASK****'
END;
```
Apply these masking policies on their respective coloumns on respective tables
```

ALTER TABLE IF EXISTS employee_internal MODIFY EMAIL SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee_external MODIFY EMAIL SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee_internal MODIFY mobile_number SET MASKING POLICY contact_mask;
ALTER TABLE IF EXISTS employee_external MODIFY mobile_number SET MASKING POLICY contact_mask;
```
## Using admin role

<img width="1124" alt="Screenshot 2023-05-18 at 6 38 27 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/2acf2390-27c3-4015-ab2f-91846237a33b">

## Using PII role
```
USE ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external TO ROLE PII;
USE ROLE PII;
```
<img width="1120" alt="Screenshot 2023-05-18 at 6 40 33 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/98a68a50-2971-4993-a6c4-1c8be528bc66">

## Using Developer role

```
USE ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_internal TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_external TO ROLE DEVELOPER;
USE ROLE DEVELOPER;

```
<img width="1119" alt="Screenshot 2023-05-18 at 6 41 55 PM" src="https://github.com/rithish1126/Snow_Flake_Assignment/assets/122535424/50c41437-3d52-473c-bd2f-340c8633105c">
