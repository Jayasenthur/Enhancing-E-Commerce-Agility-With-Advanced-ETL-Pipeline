import json
import boto3
import re

def lambda_handler(event, context):
    glue = boto3.client('glue')

    # Expect input from check-orders-returns-exist
    bucket_orders = event.get('bucket_orders')
    bucket_returns = event.get('bucket_returns')
    key_orders = event.get('key_orders')
    key_returns = event.get('key_returns')

    if not all([bucket_orders, bucket_returns, key_orders, key_returns]):
        return {
            'statusCode': 400,
            'body': 'Missing required S3 file details'
        }

    job_name = "ecommerce_join_data"

    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--bucket_orders': bucket_orders,
                '--bucket_returns': bucket_returns,
                '--key_orders': key_orders,
                '--key_returns': key_returns,
                '--output_database': 'ecommerce_analytics',  # New parameter
                '--output_table': 'joined_orders_returns',  # New parameter
                '--job-language': 'python',  # Recommended for Python jobs
                '--job-bookmark-option': 'job-bookmark-enable'  # Optional: enables job bookmarking
            }
        )

        job_run_id = response['JobRunId']  # Fixed casing (was job_run_id)

        return {
            'statusCode': 200,
            'JobName': job_name,
            'JobRunId': job_run_id,
            'message': 'Glue job successfully started with Data Catalog integration'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'error': str(e),
            'context': 'Failed to start Glue job with Data Catalog parameters'
        }
