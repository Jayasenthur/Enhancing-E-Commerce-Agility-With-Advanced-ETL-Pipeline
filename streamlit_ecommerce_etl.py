import streamlit as st
import boto3
import json
import time
from datetime import datetime
import pandas as pd
from io import StringIO, BytesIO

# AWS Configuration
AWS_CONFIG = {
    #'aws_access_key_id': "AKIA2VKY4KNOCFGW3BNG",
    #'aws_secret_access_key': "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",
    'region_name': "us-east-1"
}

# AWS Resource Configuration
BUCKET_ORDERS = 'ecommerce-orders-raw'
BUCKET_RETURNS = 'ecommerce-returns-raw'
PROCESSED_BUCKET = 'ecommerce-processed'
GLUE_DATABASE = 'default'
GLUE_TABLE = 'joined_data'
STEP_FUNCTION_ARN = 'arn:aws:states:us-east-1:733015200604:stateMachine:ETL-StepFunction-Controller'

# Initialize AWS clients
@st.cache_resource
def get_aws_clients():
    try:
        return {
            's3': boto3.client('s3', **AWS_CONFIG),
            'stepfunctions': boto3.client('stepfunctions', **AWS_CONFIG),
            'glue': boto3.client('glue', **AWS_CONFIG)
        }
    except Exception as e:
        st.error(f"❌ Failed to initialize AWS clients: {str(e)}")
        st.stop()

clients = get_aws_clients()

# UI Components
st.title("📦 E-commerce Analytics Pipeline")
st.markdown("""
Upload order and return files to trigger the ETL pipeline.  
The data will be processed and stored in AWS Glue Data Catalog.
""")

# File upload section
with st.expander("📤 Upload Files", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        uploaded_orders = st.file_uploader("Orders CSV", type=["csv"], key="orders")
    with col2:
        uploaded_returns = st.file_uploader("Returns CSV", type=["csv"], key="returns")

def upload_to_s3(file_obj, bucket, prefix):
    """Upload file to S3 with timestamp"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.csv"
        
        # Convert BytesIO to file-like object if needed
        if isinstance(file_obj, BytesIO):
            file_obj.seek(0)
        
        clients['s3'].upload_fileobj(file_obj, bucket, filename)
        return filename, None
    except Exception as e:
        return None, str(e)

def trigger_step_function(input_data):
    """Start Step Function execution"""
    try:
        response = clients['stepfunctions'].start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps(input_data))
        return response['executionArn'], None
    except Exception as e:
        return None, str(e)

def get_execution_status(execution_arn):
    """Check Step Function execution status"""
    try:
        response = clients['stepfunctions'].describe_execution(
            executionArn=execution_arn)
        return response['status'], response.get('output', '{}')
    except Exception as e:
        return 'FAILED', str(e)

def get_latest_joined_data():
    """Get the most recent joined data from S3"""
    try:
        # List objects in the processed bucket
        objects = clients['s3'].list_objects_v2(
            Bucket=PROCESSED_BUCKET,
            Prefix="joined-data/"
        )
        
        if not objects.get('Contents'):
            return None, "No processed data found"
            
        # Get the most recent file
        latest_file = max(objects['Contents'], key=lambda x: x['LastModified'])
        obj = clients['s3'].get_object(Bucket=PROCESSED_BUCKET, Key=latest_file['Key'])
        
        # Read as DataFrame (assuming Parquet format)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        return df, None
    except Exception as e:
        return None, str(e)

# Main execution flow
if uploaded_orders and uploaded_returns:
    # Upload files
    with st.spinner("Uploading files to S3..."):
        # Convert to BytesIO if they're UploadedFile objects
        orders_file = BytesIO(uploaded_orders.getvalue())
        returns_file = BytesIO(uploaded_returns.getvalue())
        
        orders_key, orders_err = upload_to_s3(orders_file, BUCKET_ORDERS, "orders")
        returns_key, returns_err = upload_to_s3(returns_file, BUCKET_RETURNS, "returns")
    
    if orders_err or returns_err:
        st.error(f"Upload errors - Orders: {orders_err or 'OK'}, Returns: {returns_err or 'OK'}")
    else:
        st.success(f"Files uploaded successfully to S3!")
        st.write(f"Orders file: s3://{BUCKET_ORDERS}/{orders_key}")
        st.write(f"Returns file: s3://{BUCKET_RETURNS}/{returns_key}")
        
        # Trigger Step Function
        input_data = {
            "bucket_orders": BUCKET_ORDERS,
            "bucket_returns": BUCKET_RETURNS,
            "key_orders": orders_key,
            "key_returns": returns_key,
            "output_database": GLUE_DATABASE,
            "output_table": GLUE_TABLE
        }
        
        execution_arn, sf_err = trigger_step_function(input_data)
        
        if sf_err:
            st.error(f"Failed to trigger pipeline: {sf_err}")
        else:
            st.success("ETL pipeline started!")
            st.write(f"Execution ARN: `{execution_arn}`")
            
            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i in range(1, 101):
                time.sleep(1)
                progress_bar.progress(i)
                status, output = get_execution_status(execution_arn)
                status_text.markdown(f"**Status:** `{status}`")
                
                if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
                    if status == 'SUCCEEDED':
                        st.balloons()
                        st.success("ETL Pipeline completed successfully!")
                        
                        # Show preview after short delay
                        time.sleep(2)  # Give S3 time to stabilize
                        with st.spinner("Loading preview data..."):
                            df, preview_err = get_latest_joined_data()
                            if preview_err:
                                st.warning(f"Couldn't load preview: {preview_err}")
                            else:
                                st.subheader("Joined Data Preview")
                                st.dataframe(df.head(20))
                                
                                # Download button
                                csv = df.head(100).to_csv(index=False)
                                st.download_button(
                                    label="Download Sample",
                                    data=csv,
                                    file_name='joined_data_sample.csv',
                                    mime='text/csv'
                                )
                    else:
                        st.error(f"Pipeline failed: {status}")
                    break

# Manual refresh for preview
if st.button("🔄 Refresh Data Preview"):
    with st.spinner("Loading latest data..."):
        df, preview_err = get_latest_joined_data()
        if preview_err:
            st.warning(f"Couldn't load preview: {preview_err}")
        else:
            st.subheader("Joined Data Preview")
            st.dataframe(df.head(20))