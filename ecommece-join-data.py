import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import current_timestamp, col, to_date, lit
from awsglue.job import Job

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# Get job arguments - NOTE THE FOUR DASHES (----) FOR GLUE NATIVE JOBS
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bucket_orders',
    'bucket_returns',
    'key_orders',
    'key_returns',
    'output_database',
    'output_table'
])

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # S3 Paths - access with FOUR dashes
    orders_path = f"s3://{args['bucket_orders']}/{args['key_orders']}"
    returns_path = f"s3://{args['bucket_returns']}/{args['key_returns']}"
    
    logger.info(f"Reading orders from: {orders_path}")
    logger.info(f"Reading returns from: {returns_path}")

    # Read CSVs
    orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)
    returns_df = spark.read.csv(returns_path, header=True, inferSchema=True)

    # Data validation
    required_order_cols = {'Order ID', 'Customer ID', 'Order Date'}
    required_return_cols = {'Order ID', 'Return Date', 'Return Reason'}
    
    for df, cols in [(orders_df, required_order_cols), (returns_df, required_return_cols)]:
        missing = cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    # Transformations
    orders_df = orders_df \
        .withColumnRenamed('Order ID', 'order_id') \
        .withColumnRenamed('Order Date', 'order_date') \
        .withColumn('order_source', lit('orders')) \
        .withColumn('order_date', to_date(col('order_date'), 'yyyy-MM-dd'))
    
    returns_df = returns_df \
        .withColumnRenamed('Order ID', 'order_id') \
        .withColumnRenamed('Return Date', 'return_date') \
        .withColumn('return_source', lit('returns')) \
        .withColumn('return_date', to_date(col('return_date'), 'yyyy-MM-dd'))

    # Join data
    joined_df = orders_df.join(returns_df, 'order_id', 'left')
    
    # Add timestamp
    joined_df = joined_df.withColumn("processing_timestamp", current_timestamp())

    if not joined_df.rdd.isEmpty():
        # Write to S3
        joined_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save("s3://ecommerce-processed/joined-data/")
        
        logger.info("Successfully wrote to s3://ecommerce-processed/joined-data/")
    else:
        logger.warn("No data to write - joined DataFrame is empty")

    job.commit()
    logger.info("Job completed successfully")

except Exception as e:
    logger.error(f"Job failed: {str(e)}")
    job.commit()
    raise