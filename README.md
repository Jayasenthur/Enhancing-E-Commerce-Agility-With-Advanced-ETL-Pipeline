# Enhancing-E-Commerce-Agility-With-Advanced-ETL-Pipeline
## Project Overview
This project automates the processing of __orders__ and __returns__ data stored in an S3 bucket, performs transformations using AWS Glue, and orchestrates the workflow using AWS Step Functions. The processed data is then made available for visualization via a Streamlit application.

## Problem Statement
As a Data Engineer Your objective is to build an end-to-end automated data processing workflow that handles data uploads from the Order and Returns teams, performs a join operation using Glue & PySpark, stores the joined data in Redshift, and sends notifications about the pipeline's status using SNS. 

## Project Workflow: End-to-End Data Pipeline

## Step 1: Data Upload via Streamlit
* Order team uploads __Order__ data file via Streamlit.
* Returns team uploads __Returns__ data file via Streamlit.
* Files are securely uploaded to their respective S3 buckets:
  * `ecommerce-orders-raw` for Order file
  * `ecommerce-returns-raw` for Return file
 
## Step 2: Lambda Trigger on S3 Upload
* Each file upload triggers an AWS Lambda function.
* Lambda:
   * Identifies which file was uploaded
   * Starts an AWS Glue ETL job both the order and return files are uploaded.

## Step 3: Data Processing with AWS Glue
* Glue ETL job performs:
  * Data extraction from S3 buckets
  * Data transformation using PySpark
  * Join operation on "Order ID" from both datasets.
* The final joined dataset is prepared.

## Step 4: Data Storage and Access
* The joined data is written to Data catalog instead of Redshift
* Teams can query the joined data

## Step 5: Orchestration with AWS Step Functions
* AWS Step Functions manages the full process:
  * Wait for file upload
  * Trigger Lambda
  * Launch Glue Job
  * Load data to Data catalog
  * Notify status
* Monitors each stage and handles errors or retries.

## Step 6: Status Display in Streamlit UI
* Streamlit shows pipeline execution status (Success or Failure).
* This gives real-time feedback to the teams after data upload.

## Step 7: Notifications via Amazon SNS
* After the pipeline finishes:
   * An SNS topic sends an email.
   * Subscribers receive the notification.
   * Email indicates whether the pipeline succeeded or failed.
