# Enhancing-E-Commerce-Agility-With-Advanced-ETL-Pipeline
## Project Overview
This project automates the processing of __orders__ and __returns__ data stored in an S3 bucket, performs transformations using AWS Glue, and orchestrates the workflow using AWS Step Functions. The processed data is then made available for visualization via a Streamlit application.

## Problem Statement
As a Data Engineer Your objective is to build an end-to-end automated data processing workflow that handles data uploads from the Order and Returns teams, performs a join operation using Glue & PySpark, stores the joined data in Redshift, and sends notifications about the pipeline's status using SNS. 

## Technologies used :

- AWS Glue
- pyspark
- SNS
- Step Funtion
- S3
- Data Catalog and Athena (Replaced Redshift)
- Streamlit

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
   * Starts an AWS Glue ETL job.

## Step 3: Data Processing with AWS Glue
* Glue ETL job performs:
  * Data extraction from S3 buckets
  * Data transformation using PySpark
  * Join operation on "Order ID" from both datasets.
* The final joined dataset is prepared.

## Step 4: Data Storage and Access
* The joined data is written to Data catalog instead of Redshift
* Teams can query the joined data with Athena

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

## Workflow Diagram

 ![Workflow Diagram](https://github.com/Jayasenthur/Enhancing-E-Commerce-Agility-With-Advanced-ETL-Pipeline/blob/main/workflow/ecommerceETL.png)

## Setup and Core Components
## 1. AWS Account Setup
   * Create an AWS account
   * Set up IAM user with appropriate permissions (S3, Glue, Lambda, Step Functions, Data catalog, SNS)
   * Policies attached :
     * `AmazonS3FullAccess`
     * `AWSLambda_FullAccess`
     * `AWSStepFunctionsFullAccess`
     * CustomInlinePolicy
    
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::ecommerce-orders-raw",
                "arn:aws:s3:::ecommerce-orders-raw/*",
                "arn:aws:s3:::ecommerce-returns-raw",
                "arn:aws:s3:::ecommerce-returns-raw/*",
                "arn:aws:s3:::ecommerce-processed",
                "arn:aws:s3:::ecommerce-processed/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "sns:Publish",
            "Resource": [
                "arn:aws:sns:us-east-1:733015200604:etl-success-topic",
                "arn:aws:sns:us-east-1:733015200604:etl-failure-topic"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:StartExecution",
                "states:DescribeExecution"
            ],
            "Resource": [
                "arn:aws:states:us-east-1:733015200604:stateMachine:ETL-StepFunction-Controller",
                "arn:aws:states:us-east-1:733015200604:execution:ETL-StepFunction-Controller:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTable",
                "glue:GetDatabase"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:733015200604:table/default/joined_data",
                "arn:aws:glue:us-east-1:733015200604:database/default",
                "arn:aws:glue:us-east-1:733015200604:catalog"
            ]
        }
    ]
}
```
## 2. Create S3 Buckets
* `ecommerce-orders-raw` - for order team uploads
* `ecommerce-returns-raw` - for returns team uploads
* `ecommerce-processed/joined-data` - for joined data

### Configure S3 Triggers
* Create the S3 event trigger `trigger-lambda-on-orders-upload` which automatically invokes your Lambda function `ecommerce-trigger-glue` whenever a new file is uploaded to the `ecommerce-orders-raw` bucket, which then starts your Glue ETL job to process the data.
*  Create the S3 event trigger `trigger-lambda-on-returns-upload` which automatically invokes your Lambda function `ecommerce-trigger-glue` whenever a new file is uploaded to the `ecommerce-returns-raw` bucket, which then starts your Glue ETL job to process the data.

## 3. Lambda Function Setup
## 1. Create Lambda Function
  * Name: `ecommerce-trigger-glue`
  * Runtime: Python 3.9
  * Permissions: Glue start-job-run, SNS, CloudWatch logs
  * Inline policy attached
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "LambdaInvokePermissions",
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "arn:aws:lambda:us-east-1:733015200604:function:ecommerce-trigger-glue"
        },
        {
            "Sid": "GlueJobRunPermissions",
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJob",
                "glue:GetJobRuns"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:733015200604:job/ecommerce_join_data"
            ]
        },
        {
            "Sid": "CloudWatchLogsPermissions",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:733015200604:log-group:/aws/lambda/ecommerce-trigger-glue:*"
            ]
        },
        {
            "Sid": "SNSPublishPermissions",
            "Effect": "Allow",
            "Action": "sns:Publish",
            "Resource": [
                "arn:aws:sns:us-east-1:733015200604:etl-success-topic",
                "arn:aws:sns:us-east-1:733015200604:etl-failure-topic"
            ]
        }
    ]
}
```
### Purpose
This AWS Lambda function is triggered when both Order and Returns files are uploaded to S3. It:
* Extracts S3 bucket and object key details from the event.
* Starts an AWS Glue job named `ecommerce_join_data` with the file locations and additional parameters like output database and table.
* Returns the Glue job run ID if successful, or an error message if something goes wrong.

## 2. Create Lambda Function
  * Name: `check-orders-returns-exist`
  * Runtime: Python 3.9
  * Permissions: S3, CloudWatch logs
  * Inline policy attached
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowCreateLogGroup",
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:us-east-1:733015200604:*"
        },
        {
            "Sid": "AllowCreateLogStreamAndPutEvents",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:733015200604:log-group:/aws/lambda/check-orders-returns-exist:*"
            ]
        },
        {
            "Sid": "AllowListBuckets",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::ecommerce-orders-raw",
                "arn:aws:s3:::ecommerce-returns-raw"
            ]
        },
        {
            "Sid": "AllowGetObjectsFromOrdersAndReturns",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::ecommerce-orders-raw/*",
                "arn:aws:s3:::ecommerce-returns-raw/*"
            ]
        }
    ]
}
```

### Purpose
This AWS Lambda function checks whether the latest Order and Returns CSV files exist in their respective S3 buckets:
* Check whether the latest orders and returns files exist in two different S3 buckets (ecommerce-orders-raw and ecommerce-returns-raw).
* If both files exist, it returns their S3 bucket names and file keys.
* If either file is missing or inaccessible, it returns "both_exist": False.

It’s used as a pre-check step in a data pipeline to ensure that the required data files are present in S3  before triggering the Glue ETL process.

## 3. Create Lambda Function
  * Name: `check-glue-job-status`
  * Runtime: Python 3.9
  * Permissions: S3, Glue, CloudWatch logs, SNS, Lamda
  * Inline policy attached
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": "logs:CreateLogGroup",
			"Resource": "arn:aws:logs:us-east-1:733015200604:*"
		},
		{
			"Effect": "Allow",
			"Action": [
				"logs:CreateLogStream",
				"logs:PutLogEvents"
			],
			"Resource": [
				"arn:aws:logs:us-east-1:733015200604:log-group:/aws/lambda/check-glue-job-status:*"
			]
		},
		{
			"Effect": "Allow",
			"Action": "glue:GetJobRun",
			"Resource": "arn:aws:glue:us-east-1:733015200604:job/ecommerce_join_data"
		},
		{
			"Effect": "Allow",
			"Action": "SNS:Publish",
			"Resource": [
				"arn:aws:sns:us-east-1:733015200604:etl-success-topic",
				"arn:aws:sns:us-east-1:733015200604:etl-failure-topic"
			]
		},
		{
			"Effect": "Allow",
			"Action": "lambda:InvokeFunction",
			"Resource": "arn:aws:lambda:us-east-1:733015200604:function:check-glue-job-status"
		}
	]
}
```
### Purpose ### 
This Lamda function accepts
  - Accepts a `JobRunId` from the event input.
  - Checks the status of a Glue job run
  - Sends an SNS notification if the job is succeeded or failed
  - Returns the current job status.
    
## 4. Glue ETL Job 
### 1.Create Glue Job
- Name: `ecommerce-join-data`
- Type: Spark
- IAM Role: `AWSGlueServiceRole`
- Permissions attached for IAM role
  * `AmazonS3FullAccess`
  * `AWSGlueConsoleFullAccess`
  * `AWSGlueServiceRole`
  * Inline policy attached : Glue,S3
```json
 "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:PutObjectAcl",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::ecommerce-orders-raw/*",
                "arn:aws:s3:::ecommerce-returns-raw/*",
                "arn:aws:s3:::ecommerce-processed/*",
                "arn:aws:s3:::ecommerce-processed/joined-data/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTable",
                "glue:UpdateTable",
                "glue:CreateTable"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:733015200604:table/default/joined_data",
                "arn:aws:glue:us-east-1:733015200604:database/default"
            ]
        },
```

### 2.Glue PySpark Script
This AWS Glue PySpark script performs an ETL job to join orders and returns data from S3 and save the result in Parquet format:
1. Reads input arguments (S3 buckets, keys, output database/table).
2. Loads CSV files from `ecommerce-orders-raw` and `ecommerce-returns-raw`
3. Validates required columns exist in both datasets.
4. Renames and transforms columns, converting date fields and adding source tags.
5. Joins the orders and returns on `order_id` (left join).
6. Adds a processing timestamp and writes the final data to `s3://ecommerce-processed/joined-data/` in Parquet format.
7. Commits the Glue job and logs success or failure.

## 5. Data storage - Why Athena Replaced Redshift
In our initial ETL pipeline, we used Amazon Redshift for data warehousing. However, after evaluating performance, cost, and scalability, we decided to migrate to AWS Glue Data Catalog and Amazon Athena for the following reasons:

## Benefits of AWS Glue Data Catalog & Athena Over Redshift

| **Factor**          | **Redshift** | **Athena + Glue Data Catalog** |
|---------------------|-------------|--------------------------------|
| **Cost**           | Expensive (provisioned clusters) | Pay-per-query (serverless) |
| **Scalability**    | Manual scaling required | Automatically scales with data volume |
| **Maintenance**    | Requires tuning, vacuuming, and administration | Fully managed (no maintenance) |
| **Query Performance** | Optimized for complex OLAP queries | Best for ad-hoc SQL queries on S3 |
| **Data Format Support** | Limited (requires loading) | Supports Parquet, JSON, CSV, ORC directly in S3 |
| **ETL Integration** | Needs separate ETL jobs | Seamless with AWS Glue |

### Use Case Fit
- Our e-commerce data is stored in S3 (raw and transformed).
- Most queries are ad-hoc analytics, not heavy joins or aggregations.
- We wanted a serverless solution to reduce operational overhead.

### 1. Setting Up AWS Glue Data Catalog from S3
### Prerequisites
- An S3 bucket with structured data (e.g., Parquet, JSON, CSV).
- IAM permissions for AWS Glue and Athena.

### 2. Steps to Create a Data Catalog
### Using AWS Glue Crawler (Automated Schema Detection)
1. Go to AWS Glue Console → Crawlers → Add Crawler.
2. Configure Crawler:
   - Name: `ecommerce-data-crawler`
   - Data Source: S3 path (`s3://ecommerce-processed/joined-data/`)
   - IAM Role: `AWSGlueServiceRole`
3. Configure Output:
   - Database: ecommerce_db (create if it doesn’t exist)

### Querying Data with Amazon Athena
Athena allows SQL queries directly on S3 data using the Glue Data Catalog.
```sql
SELECT * FROM "default"."joined_data" LIMIT 10;
```

## 6. Orchestration - Step Function
This AWS Step Functions state machine coordinates an automated ETL pipeline with file existence checks, retries, and notifications. Here's a simplified explanation of what it does:
1. ## Initialize Retry Counter
     * Starts the flow by initializing a retry counter to zero.
2. ## Check for Files
     * Invokes the Lambda function `check-orders-returns-exist` to verify if both the orders and returns files exist in S3.
3. ## Decision - FilesExist?
     * If both files exist → move to trigger the Glue job.
     * If not and retry count < 5 → wait 30 seconds, increment retry, and recheck.
     * If retry count ≥ 5 → fail the execution.
4. ## Trigger Glue Job
     * If files are found, it calls another Lambda function `ecommerce-trigger-glue` to start the AWS Glue job.
5. ## Check Glue Job Status
     * After triggering, it periodically checks the status of the Glue job using the `check-glue-job-status` Lambda function.
6. ## Glue Job Status Check
     * If Glue job succeeds → send success notification to SNS.
     * If fails → send failure notification.
     * If still in progress → wait 60 seconds and recheck.
7. ## Error Handling
     * Any failure in Lambda or Glue job triggers the HandleLambdaError state, which ultimately leads to a failure notification.
8. ## Notifications
     * SuccessNotification: Publishes a success message to the `etl-success-topic` SNS topic.
     * FailExecution: Publishes a failure message (with details like which file failed, Glue status, timestamp) to the `etl-failure-topic`.
9. ## Retry Logic Summary
     * Retries the file check up to 5 times, waiting 30 seconds between attempts, to handle delays in S3 uploads.

This Step Function automates ETL execution robustly, ensuring:

* It only runs the Glue job when both files are ready,
* It handles temporary file delays with retries,
* It monitors job success/failure,
* It notifies stakeholders via SNS.

## Step function Diagram

 ![Step function Diagram](https://github.com/Jayasenthur/Enhancing-E-Commerce-Agility-With-Advanced-ETL-Pipeline/blob/main/Stepfunc/stepfunctions_graph.png)


## 6. StreamlitUI

Streamlit web app that allows users to:
* Upload orders and returns CSV files.
* Store them in AWS S3 buckets.
* Trigger an AWS Step Function to run an ETL pipeline.
* After success, it loads the processed, joined data from S3.
* Displays a preview of joined data and gives a download option.

**Tech Stack**
* Streamlit for web UI
* AWS S3 for file storage
* AWS Step Functions to trigger ETL
* AWS Glue for data processing
* Pandas to show the data

## StreamlitUI Output

![Streamlit](https://github.com/Jayasenthur/Enhancing-E-Commerce-Agility-With-Advanced-ETL-Pipeline/blob/main/output/streamlitoutput.png)


## Why Use CloudWatch?

CloudWatch is AWS’s built-in monitoring service that helps you:

- Detect failures in real-time (Glue jobs, Lambda, Step Functions).
- Debug errors with detailed logs.
- Set up alerts (e.g., email/SMS when something breaks).
- Track performance (e.g., slow Glue jobs or Lambda timeouts).
- Execution history (which step failed).

## Project Challenges
## 1. Cost Overruns
### Challenge :
- Redshift was expensive for ad-hoc queries.
### Solution:
- Migrated to Athena + Glue Data Catalog (saved ~70% costs).
- Converted data to Parquet (reduced query costs by 80%).

## 2. Glue Job Failures
### Challenge:
- PySpark scripts failing due to schema mismatches (e.g., missing columns in raw CSVs).
### Solution:
- Added data validation in Glue (e.g., df.printSchema() + mandatory column checks).

## 3. Glue Job Output Not Appearing in S3
### Challenge :
Joined data not visible in `s3://ecommerce-processed/joined-data/` despite job success.

### Solution :
* __Verify the output path__ in your PySpark script:
```python
output_path = "s3://ecommerce-processed/joined-data/"  # Must match bucket
```
* __Check IAM permissions__:
- Fixed IAM permissions for Glue to write to S3.

## 4. Lambda Trigger Issues

### Challenge :
- Lambda not triggering reliably when files landed in S3.

### Solution :
- Used S3 Event Notifications + added a `check-orders-returns-exist` Lambda to verify file pairs.

