# STEDI Human Balance Analytics

This project aims to build a lakehouse solution in AWS using AWS Glue, AWS S3, Python, and Spark. The solution will satisfy the requirements provided by the STEDI data scientists.

## Project Instructions

To complete the project, follow these steps:

### 1. Data Preparation
- Download data from here: https://video.udacity-data.com/topher/2022/June/62be2ed5_stedihumanbalanceanalyticsdata/stedihumanbalanceanalyticsdata.zip
- Create S3 directories for the landing zones: `customer/landing`, `step_trainer/landing`, and `accelerometer/landing`. Copy the respective data files into these directories as a starting point.

### 2. Glue Table Creation

- Create two Glue tables using AWS Glue for the landing zones. Provide the SQL scripts for creating these tables: `customer_landing.sql` and `accelerometer_landing.sql`. These scripts define the schema and structure of the tables.

### 3. Data Querying with Athena

- Use Athena to query the created Glue tables: `customer_landing` and `accelerometer_landing`.
- Take screenshots of the query results for each table and save them as `customer_landing.png` and `accelerometer_landing.png`, respectively.

### 4. Step working on Glue

  1. Create a Glue job: from S3 `customer/landing/`, filter datas have `sharewithresearchasofdate != 0`, store to S3 `customer/trusted/`
  2. Create a Glue table called `customer_trusted` from S3 `customer/trusted/`
  3. Create a Glue job: join S3 `accelerometer/landing/` and Glue table customer_trusted by `email == user`, drop fields in customer table, store to S3 `accelerometer/trusted/`
  4. Create a Glue table called `accelerometer_trusted` from S3 `accelerometer/trusted/`
  5. Create a Glue job: join S3 `accelerometer/landing/` and Glue table `customer_trusted` by `email == user`, drop fields in accelerometer table, store to S3 `customer/curated/`
  6. Create a Glue table called `customer_curated` from S3 `customer/curated/`
  7. Create a Glue job: join S3 `step_trainer/landing/` and Glue table `customer_curated` by `serialnumber`, drop fields in `customer_curated` table, store to S3 `step_trainer/trusted/`
  8. Create a Glue table called `step_trainer_trusted` from S3 `step_trainer/trusted/`
  9. Create a Glue job: join Glue table accelerometer_trusted and Glue table `step_trainer_trusted` by `timestamp == sensorreadingtime`, store to S3 `machine_learning_curated/`
  10. Create a Glue table called `machine_learning_curated` from S3 `machine_learning_curated/`

### 5. Data Verification with Athena

- Use Athena to query the `customer_trusted` Glue table.
- Take a screenshot of the query result and save it as `customer_trusted.png`.

## Project Submission Included

1. `customer_landing.sql`: the SQL script for creating `customer_landing` table
2. `accelerometer_landing.sql`: the SQL script for creating `accelerometer_landing` table
3. `accelerometer_landing.png`:  the screenshot of the Athena query results for `accelerometer_landing` table
4. `customer_landing.png`:  the screenshot of the Athena query results for `customer_landing` table
5. `Customer Landing to Trusted.py`: the Glue job script for sanitizing the customer data and store only customers who agreed to share their data for research purposes to `customer_trusted`
6. `customer_trusted.png`: the screenshot of the Athena query results for `customer_trusted` table
7. `customer_trusted_with_sharewithresearchasofdate_is_NULL.png`: the screenshot of the Athena query results for `customer_trusted` table, which included condition sharewithresearchasofdate is NULL
8. `Accelerometer Landing To Trusted.py`: the Glue job script for sanitizing the accelerometer data - and only store Accelerometer Readings from customers who agreed to share their data for research purposes
9. `Customer Trusted To Curated.py`: the Glue job script for sanitizing the customer data - and only store customers who have accelerometer data and have agreed to share their data for research
10. `Step Trainer Landing to Trusted.py`: the Glue job script for sanitizing the Step Trainer IoT data stream (S3) that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research
11. `Machine Learning Curated.py`: the Glue job script for sanitizing the Step Trainer IoT data stream and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data
