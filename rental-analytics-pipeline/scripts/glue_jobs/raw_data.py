import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Relational DB
RelationalDB_node1750033294897 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "bookings",
        "connectionName": "Aurora connection 2",
    },
    transformation_ctx = "RelationalDB_node1750033294897"
)

# Script generated for node Relational DB
RelationalDB_node1750059325735 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "user_viewing",
        "connectionName": "Aurora connection 2",
    },
    transformation_ctx = "RelationalDB_node1750059325735"
)

# Script generated for node Relational DB
RelationalDB_node1750033060431 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartments",
        "connectionName": "Aurora connection 2",
    },
    transformation_ctx = "RelationalDB_node1750033060431"
)

# Script generated for node Relational DB
RelationalDB_node1750059051799 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartment_attributes",
        "connectionName": "Aurora connection 2",
    },
    transformation_ctx = "RelationalDB_node1750059051799"
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750033294897, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750032717768", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750033294897.count() >= 1):
   RelationalDB_node1750033294897 = RelationalDB_node1750033294897.coalesce(1)
AmazonS3_node1750033305730 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750033294897, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora-to-redshift-intermediary/raw/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750033305730")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750059325735, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750058355741", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750059325735.count() >= 1):
   RelationalDB_node1750059325735 = RelationalDB_node1750059325735.coalesce(1)
AmazonS3_node1750059340357 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750059325735, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora-to-redshift-intermediary/raw/user_viewing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750059340357")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750033060431, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750032717768", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750033060431.count() >= 1):
   RelationalDB_node1750033060431 = RelationalDB_node1750033060431.coalesce(1)
AmazonS3_node1750033079693 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750033060431, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora-to-redshift-intermediary/raw/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750033079693")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750059051799, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750058355741", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750059051799.count() >= 1):
   RelationalDB_node1750059051799 = RelationalDB_node1750059051799.coalesce(1)
AmazonS3_node1750059063709 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750059051799, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora-to-redshift-intermediary/raw/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750059063709")

job.commit()