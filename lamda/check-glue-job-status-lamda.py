import boto3
import logging

# Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
glue_client = boto3.client('glue')
sns_client = boto3.client('sns')

# Constants
GLUE_JOB_NAME = 'ecommerce_join_data'
SUCCESS_TOPIC_ARN = "arn:aws:sns:us-east-1:733015200604:etl-success-topic"
FAILURE_TOPIC_ARN = "arn:aws:sns:us-east-1:733015200604:etl-failure-topic"

def lambda_handler(event, context):
    job_run_id = event.get('JobRunId') or event.get('job_run_id')
    if not job_run_id:
        logger.error("Missing JobRunId in event: %s", event)
        raise ValueError("JobRunId not found in event payload")
    logger.info(f"Checking Glue job status for Run ID: {job_run_id}")
    
    try:
        # Fetch Glue job run status
        response = glue_client.get_job_run(
            JobName=GLUE_JOB_NAME,
            RunId=job_run_id
        )
        job_status = response['JobRun']['JobRunState']
        logger.info(f"Glue job current status: {job_status}")
        
        # Send SNS notification based on status
        if job_status == 'SUCCEEDED':
            subject = f"Glue Job {GLUE_JOB_NAME} Succeeded"
            message = f"The Glue job '{GLUE_JOB_NAME}' with Run ID '{job_run_id}' completed successfully."
            sns_client.publish(
                TopicArn=SUCCESS_TOPIC_ARN,
                Subject=subject,
                Message=message
            )
            logger.info("Success notification sent.")

        elif job_status in ['FAILED', 'STOPPED']:
            subject = f"Glue Job {GLUE_JOB_NAME} Failed"
            message = f"The Glue job '{GLUE_JOB_NAME}' with Run ID '{job_run_id}' has failed or stopped with status: {job_status}."
            sns_client.publish(
                TopicArn=FAILURE_TOPIC_ARN,
                Subject=subject,
                Message=message
            )
            logger.info("Failure notification sent.")

        return {
            'status': job_status
        }
        
    except Exception as e:
        logger.error(f"Error while checking Glue job status: {str(e)}")
        raise
