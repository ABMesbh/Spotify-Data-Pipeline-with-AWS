# Spotify ETL Project

This project is about creating an ETL (Extract, Transform, Load) pipeline using the Spotify API. The pipeline includes all steps from data extraction to loading the transformed data into Snowflake. The entire process is automated and monitored on a daily basis.

## Project Overview

1. **Extraction**:
    - **AWS Lambda**: Utilizes Python to extract data from the Spotify API. The Lambda function is triggered by a scheduled event, such as a CloudWatch Event, to run daily.
    - **S3 Bucket**: Stores the extracted data in JSON format. Each day's data is stored in a separate folder to maintain organization and facilitate easy access.

2. **Transformation**:
    - **AWS Glue**: Uses PySpark to transform the extracted data. The Glue job cleans, filters, and aggregates the data to prepare it for analysis.
    - **S3 Bucket**: Stores the transformed data in a different folder or bucket, ready for loading into Snowflake.

3. **Loading**:
    - **Snowpipe**: An automated data ingestion service provided by Snowflake. It continuously loads the transformed data from the S3 bucket into Snowflake tables.

4. **Automation and Monitoring**:
    - **AWS CloudWatch**: Automates and monitors the entire process on a daily basis. CloudWatch Events trigger the Lambda function, and CloudWatch Logs provide monitoring and alerting for the ETL pipeline.

## Objective

The main objective of this project is to monitor changes in the most played tracks on Spotify and fetch the latest trends. By automating the ETL process, we ensure that the data is always up-to-date and ready for analysis.

## Technologies Used

- **Spotify API**: Provides access to Spotify's music data, including track information, playlists, and user data.
- **AWS Lambda**: A serverless compute service that runs the Python code to extract data from the Spotify API.
- **AWS S3**: A scalable object storage service used to store both the raw extracted data and the transformed data.
- **AWS Glue**: A fully managed ETL service that uses PySpark to transform the data.
- **PySpark**: A Python API for Apache Spark, used for large-scale data processing.
- **Snowflake**: A cloud-based data warehousing service that stores the final transformed data.
- **AWS CloudWatch**: A monitoring and management service that automates and monitors the ETL pipeline.

## Workflow

1. **Data Extraction**:
    - An AWS Lambda function is triggered by a CloudWatch Event to run daily.
    - The Lambda function uses the Spotify API to extract data about the most played tracks.
    - The extracted data is saved to an S3 bucket in JSON format.

2. **Data Transformation**:
    - An AWS Glue job is triggered to transform the data using PySpark.
    - The Glue job cleans, filters, and aggregates the data.
    - The transformed data is saved to another S3 bucket.

3. **Data Loading**:
    - Snowpipe continuously monitors the S3 bucket for new data.
    - When new transformed data is detected, Snowpipe loads it into Snowflake tables.

4. **Automation**:
    - AWS CloudWatch schedules the Lambda function to run daily.
    - CloudWatch Logs monitor the execution of the Lambda function and Glue job, providing alerts in case of failures.

## Conclusion

This ETL pipeline ensures that the latest trends in Spotify's most played tracks are continuously monitored and updated, providing valuable insights into music trends. By leveraging AWS services and Snowflake, the pipeline is scalable, reliable, and easy to maintain.
