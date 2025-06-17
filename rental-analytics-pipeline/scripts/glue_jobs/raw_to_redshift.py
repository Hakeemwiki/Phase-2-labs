import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1750100575846 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/bookings/run-1750074808659-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750100575846")

# Script generated for node Amazon S3
AmazonS3_node1750099913336 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/apartments/run-1750074878792-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750099913336")

# Script generated for node Amazon S3
AmazonS3_node1750101365657 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/user_viewing/run-1750074847345-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750101365657")

# Script generated for node Amazon S3
AmazonS3_node1750098056881 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/apartment_attributes/run-1750074911014-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750098056881")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750101312994 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750100575846, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date VARCHAR, checkin_date VARCHAR, checkout_date VARCHAR, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750101312994")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750099938957 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750099913336, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp TIMESTAMP);"}, transformation_ctx="AmazonRedshift_node1750099938957")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750101400373 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750101365657, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.user_viewing", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.user_viewing (user_id INTEGER, apartment_id INTEGER, viewed_at VARCHAR, is_wishlisted BOOLEAN, call_to_action VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750101400373")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750099257512 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750098056881, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL);"}, transformation_ctx="AmazonRedshift_node1750099257512")

job.commit()