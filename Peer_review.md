# Dhruv's Approach

1) Dhruv has used create role commands and grant role commands to create roles and form relations between the roles
2) Then has used the Create Warehouse along with the size suspend time and cluster time also has been mentioned
3) while switching to admin role , all privillages are first granted to admin role to do the next tasks
4) Database called "assignment_db" has been created 
5) admin was given creating schema privilages before creating a schema called "my_schema" proceeding to then use this schema
6) Creates a local stage to upload a file that accepts a csv file format then proceeds to copy this data into a already well defined table
8) Creates a external stage with admin role and with stoarge integration capabilities to access the s3 bucket then proceeds to copy these values from s3 bucket to table already defined
10) Uploads the parquet file on the external stage while using the file format parquet and then copys it on a 
11) Wrote a masking policy on employees table and employees_external table where if role is developer then the fields like country are masked

# Arin's Approach

1) Arin creates three roles: Admin, Developer, and PII then forms relations by granting access according to hierarcy given
2) Arin then created a warehouse with medium storage size for the assignment, which is used to store the data.
3) Grants privilages to admin before switching role to admin
4) Then a database and schema are created
6) External stage was created attached with the s3 bucket which had the csv file , this data was then transfered to an employee table on    snowflake
7) A variant table was created using the parse json method from the local stage
8) Arin then creates an stage to store a parquet file of the data then sees the data in the stage 
9) Arin creates masking policies for the PII role for the email and salary columns of the employees' table, and alters the table to implement these policies.
10) Grants permissions to the Developer role to use the warehouse, database, and schema and select from the employees' table to verify masking
