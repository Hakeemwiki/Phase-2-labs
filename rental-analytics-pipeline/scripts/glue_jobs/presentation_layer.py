import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon Redshift
AmazonRedshift_node1750119022212 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT     TO_CHAR(DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')), 'YYYY-MM-DD') AS week,     COUNT(*) AS total_bookings FROM curated.bookings WHERE booking_status = 'confirmed' GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750119022212")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750118712830 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      TO_CHAR(DATE_TRUNC('week', booking_date::DATE), 'YYYY-MM-DD') AS week,     apartment_id,     SUM(total_price) AS total_revenue FROM curated.bookings WHERE booking_status = 'confirmed' GROUP BY 1, 2 ORDER BY 1, 3 DESC", "redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750118712830")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750119724858 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "WITH user_bookings AS (     SELECT          user_id,          booking_date::DATE AS booking_date,         LEAD(booking_date::DATE) OVER (PARTITION BY user_id ORDER BY booking_date::DATE) AS next_booking_date     FROM curated.bookings     WHERE booking_status = 'confirmed' ), repeat_customers AS (     SELECT DISTINCT user_id     FROM user_bookings     WHERE next_booking_date IS NOT NULL        AND DATEDIFF(day, booking_date, next_booking_date) <= 30 ) SELECT      TO_CHAR(DATE_TRUNC('month', b.booking_date::DATE), 'YYYY-MM') AS month,     COUNT(DISTINCT repeat_customers.user_id) AS repeat_customers FROM curated.bookings b JOIN repeat_customers ON b.user_id = repeat_customers.user_id GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750119724858")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750117972258 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750117972258")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750108088674 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      TO_CHAR(DATE_TRUNC('week', a.listing_created_on), 'YYYY-MM-DD') AS week,     ROUND(AVG(a.price), 2) AS avg_listing_price FROM curated.apartments a WHERE a.is_active = TRUE GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750108088674")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750111118297 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      TO_CHAR(DATE_TRUNC('month', b.checkin_date::DATE), 'YYYY-MM') AS month,     SUM(DATEDIFF(day, b.checkin_date::DATE, b.checkout_date::DATE)) AS nights_booked,     COUNT(DISTINCT b.apartment_id) * 30 AS total_available_nights,     ROUND(SUM(DATEDIFF(day, b.checkin_date::DATE, b.checkout_date::DATE))::DECIMAL            / (COUNT(DISTINCT b.apartment_id) * 30), 2) AS occupancy_rate FROM curated.bookings b WHERE b.booking_status = 'confirmed' GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750111118297")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750113020903 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750113020903")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750119547298 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      TO_CHAR(DATE_TRUNC('week', booking_date::DATE), 'YYYY-MM-DD') AS week,     ROUND(AVG(DATEDIFF(day, checkin_date::DATE, checkout_date::DATE)), 2) AS avg_booking_duration FROM curated.bookings WHERE booking_status = 'confirmed' GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750119547298")

# Script generated for node Join
Join_node1750115351218 = Join.apply(frame1=AmazonRedshift_node1750113020903, frame2=AmazonRedshift_node1750117972258, keys1=["apartment_id"], keys2=["id"], transformation_ctx="Join_node1750115351218")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
  DATE_FORMAT(DATE_TRUNC('WEEK', booking_date), 'yyyy-MM-dd') AS week,
  cityname,
  COUNT(*) AS total_bookings
FROM myDataSource
WHERE booking_status = 'confirmed'
GROUP BY 
  DATE_FORMAT(DATE_TRUNC('WEEK', booking_date), 'yyyy-MM-dd'),
  cityname
ORDER BY 
  week, 
  total_bookings DESC

'''
SQLQuery_node1750115673640 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1750115351218}, transformation_ctx = "SQLQuery_node1750115673640")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750119463881 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750119022212, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.total_bookings_per_user_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation.total_bookings_per_user_weekly (week VARCHAR, total_bookings BIGINT);"}, transformation_ctx="AmazonRedshift_node1750119463881")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750118953291 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750118712830, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.top_performing_listings_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation.top_performing_listings_weekly (week VARCHAR, apartment_id VARCHAR, total_revenue DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750118953291")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750119827338 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750119724858, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.repeat_customer_rate_monthly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation.repeat_customer_rate_monthly (month VARCHAR, repeat_customers BIGINT);"}, transformation_ctx="AmazonRedshift_node1750119827338")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750108291294 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750108088674, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.avg_listing_price_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation.avg_listing_price_weekly (week VARCHAR, avg_listing_price DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750108291294")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750112429675 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750111118297, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.occupancy_rate_monthly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation.occupancy_rate_monthly (month VARCHAR, nights_booked BIGINT, total_available_nights BIGINT, occupancy_rate DECIMAL);"}, transformation_ctx="AmazonRedshift_node1750112429675")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750119627335 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750119547298, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.avg_booking_duration_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation.avg_booking_duration_weekly (week VARCHAR, avg_booking_duration DECIMAL);"}, transformation_ctx="AmazonRedshift_node1750119627335")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750118652645 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1750115673640, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation.most_popular_locations_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation.most_popular_locations_weekly (week VARCHAR, cityname VARCHAR, total_bookings BIGINT);"}, transformation_ctx="AmazonRedshift_node1750118652645")

job.commit()