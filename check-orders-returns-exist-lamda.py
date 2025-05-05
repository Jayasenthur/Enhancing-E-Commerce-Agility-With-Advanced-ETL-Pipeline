import boto3
import logging
from botocore.exceptions import ClientError

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# Your buckets
bucket_orders = 'ecommerce-orders-raw'
bucket_returns = 'ecommerce-returns-raw'

def get_latest_file(bucket_name, prefix):
    """Fetch the latest uploaded file from S3 bucket."""
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' not in response:
        return None
    
    # Sort by LastModified descending
    files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    return files[0]['Key']

def lambda_handler(event, context):
    try:
        # ✅ Find the latest orders file (corrected to match your actual file naming convention)
        latest_orders_key = get_latest_file(bucket_orders, "ordercsv")
        if not latest_orders_key:
            logger.warning(f"No orders file found in {bucket_orders}.")
            return {"both_exist": False}
        
        # ✅ Find the latest returns file
        latest_returns_key = get_latest_file(bucket_returns, "returncsv")
        if not latest_returns_key:
            logger.warning(f"No returns file found in {bucket_returns}.")
            return {"both_exist": False}
        
        # ✅ Head check to confirm access (optional)
        s3.head_object(Bucket=bucket_orders, Key=latest_orders_key)
        s3.head_object(Bucket=bucket_returns, Key=latest_returns_key)
        
        logger.info(f"Orders file found: {latest_orders_key}")
        logger.info(f"Returns file found: {latest_returns_key}")

        return {
            "both_exist": True,
            "bucket_orders": bucket_orders,
            "bucket_returns": bucket_returns,
            "key_orders": latest_orders_key,
            "key_returns": latest_returns_key
        }
    
    except ClientError as e:
        logger.warning(f"File missing or access error: {str(e)}")
        return {"both_exist": False}
