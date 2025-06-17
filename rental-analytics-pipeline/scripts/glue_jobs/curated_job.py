import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
import gs_regex_extract
from awsglue import DynamicFrame

# Script generated for node Custom Transform
def MyTransform2(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrameCollection, DynamicFrame
    from pyspark.sql.functions import col, trim, to_date
    from pyspark.sql.types import BooleanType, DoubleType

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Cast and clean data
    df = df.withColumn("price", col("price").cast(DoubleType()))
    df = df.withColumn("is_active", col("is_active").cast(BooleanType()))
    df = df.withColumn("listing_created_on", to_date(col("listing_created_on")))
    df = df.withColumn("last_modified_timestamp", to_date(col("last_modified_timestamp")))

    # Trim strings
    df = df.withColumn("title", trim(col("title")))
    df = df.withColumn("source", trim(col("source")))
    df = df.withColumn("currency", trim(col("currency")))

    # Drop duplicates based on id and listing_created_on
    df = df.dropDuplicates(["id", "listing_created_on"])

    # Convert back to DynamicFrame
    cleaned_dyf = DynamicFrame.fromDF(df, glueContext, "cleaned_apartments")
    return DynamicFrameCollection({"CleanedApartments": cleaned_dyf}, glueContext)
# Script generated for node Custom Transform
def MyTransform4(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, to_date, when
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df = (
        df.withColumn("user_id", col("user_id").cast("string"))
          .withColumn("apartment_id", col("apartment_id").cast("string"))
          .withColumn("viewed_at", to_date(col("viewed_at"), "dd/MM/yyyy"))
          .withColumn("is_wishlisted", when(col("is_wishlisted").isNull(), False).otherwise(col("is_wishlisted")))
          .withColumn("call_to_action", when(col("call_to_action").isNull(), "Unknown").otherwise(col("call_to_action")))
    )

    # Drop duplicates where user_id and viewed_at are the same
    df_cleaned = df.dropDuplicates(["user_id", "viewed_at"])

    cleaned_dyf = DynamicFrame.fromDF(df_cleaned, glueContext, "cleaned_user_viewing")

    return DynamicFrameCollection({"CleanedUserViewing": cleaned_dyf}, glueContext)
# Script generated for node Custom Transform
def MyTransform3(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, to_date, when
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df = (
        df.withColumn("booking_id", col("booking_id").cast("string"))
          .withColumn("user_id", col("user_id").cast("string"))
          .withColumn("apartment_id", col("apartment_id").cast("string"))
          .withColumn("booking_date", to_date(col("booking_date"), "dd/MM/yyyy"))  # Adjust if needed
          .withColumn("checkin_date", to_date(col("checkin_date"), "dd/MM/yyyy"))
          .withColumn("checkout_date", to_date(col("checkout_date"), "dd/MM/yyyy"))
          .withColumn("total_price", col("total_price").cast("double"))
          .withColumn("currency", when(col("currency").isNotNull(), col("currency")).otherwise("USD"))
          .withColumn("booking_status", when(col("booking_status").isNull(), "unknown").otherwise(col("booking_status")))
    )

    # Filter rows with inconsistent dates
    df_cleaned = df.filter(
        (col("checkout_date").isNull()) | 
        (col("checkin_date").isNull()) | 
        (col("checkout_date") >= col("checkin_date"))
    )

    # Drop duplicates if booking_id and booking_date are the same
    df_cleaned = df_cleaned.dropDuplicates(["booking_id", "booking_date"])

    cleaned_dyf = DynamicFrame.fromDF(df_cleaned, glueContext, "cleaned_bookings")

    return DynamicFrameCollection({"CleanedBookings": cleaned_dyf}, glueContext)
# Script generated for node Custom Transform
def MyTransform1(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrameCollection, DynamicFrame
    from pyspark.sql.functions import col, regexp_replace, trim
    from pyspark.sql.types import DoubleType, BooleanType, IntegerType

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Remove dollar sign explicitly from price_display
    df = df.withColumn(
        "price",
        regexp_replace(col("price_display"), "\\$", "").cast(DoubleType())
    )

    # Clean boolean and numeric fields
    df = df.withColumn("has_photo", col("has_photo").cast(BooleanType()))
    df = df.withColumn("pets_allowed", col("pets_allowed").cast(BooleanType()))
    df = df.withColumn("bathrooms", col("bathrooms").cast(IntegerType()))
    df = df.withColumn("bedrooms", col("bedrooms").cast(IntegerType()))
    df = df.withColumn("fee", col("fee").cast(DoubleType()))
    df = df.withColumn("square_feet", col("square_feet").cast(IntegerType()))

    # Trim strings
    df = df.withColumn("address", trim(col("address")))
    df = df.withColumn("cityname", trim(col("cityname")))
    df = df.withColumn("state", trim(col("state")))

    # Drop duplicates by ID + address
    df = df.dropDuplicates(["id", "address"])

    # Convert back to DynamicFrame
    cleaned_dyf = DynamicFrame.fromDF(df, glueContext, "cleaned_apartment_attributes")
    return DynamicFrameCollection({"CleanedApartmentAttributes": cleaned_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1750083929990 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/apartments/run-1750074878792-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750083929990")

# Script generated for node Amazon S3
AmazonS3_node1750089212224 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/user_viewing/run-1750074847345-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750089212224")

# Script generated for node Amazon S3
AmazonS3_node1750073468935 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/apartment_attributes/"], "recurse": True}, transformation_ctx="AmazonS3_node1750073468935")

# Script generated for node Amazon S3
AmazonS3_node1750088850059 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora-to-redshift-intermediary/raw/bookings/run-1750074808659-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750088850059")

# Script generated for node Custom Transform
CustomTransform_node1750087994584 = MyTransform2(glueContext, DynamicFrameCollection({"AmazonS3_node1750083929990": AmazonS3_node1750083929990}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750089400972 = MyTransform4(glueContext, DynamicFrameCollection({"AmazonS3_node1750089212224": AmazonS3_node1750089212224}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750080156093 = MyTransform1(glueContext, DynamicFrameCollection({"AmazonS3_node1750073468935": AmazonS3_node1750073468935}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750089053113 = MyTransform3(glueContext, DynamicFrameCollection({"AmazonS3_node1750088850059": AmazonS3_node1750088850059}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1750088548758 = SelectFromCollection.apply(dfc=CustomTransform_node1750087994584, key=list(CustomTransform_node1750087994584.keys())[0], transformation_ctx="SelectFromCollection_node1750088548758")

# Script generated for node Select From Collection
SelectFromCollection_node1750089607108 = SelectFromCollection.apply(dfc=CustomTransform_node1750089400972, key=list(CustomTransform_node1750089400972.keys())[0], transformation_ctx="SelectFromCollection_node1750089607108")

# Script generated for node Select From Collection
SelectFromCollection_node1750080459195 = SelectFromCollection.apply(dfc=CustomTransform_node1750080156093, key=list(CustomTransform_node1750080156093.keys())[0], transformation_ctx="SelectFromCollection_node1750080459195")

# Script generated for node Select From Collection
SelectFromCollection_node1750089156783 = SelectFromCollection.apply(dfc=CustomTransform_node1750089053113, key=list(CustomTransform_node1750089053113.keys())[0], transformation_ctx="SelectFromCollection_node1750089156783")

# Script generated for node Regex Extractor
RegexExtractor_node1750082918952 = SelectFromCollection_node1750080459195.gs_regex_extract(colName="price_display", regex="\$?(\d+\.?\d*)", newCols="price_display_new")

# Script generated for node Change Schema
ChangeSchema_node1750083050741 = ApplyMapping.apply(frame=RegexExtractor_node1750082918952, mappings=[("id", "int", "id", "int"), ("category", "string", "category", "string"), ("body", "string", "body", "string"), ("amenities", "string", "amenities", "string"), ("bathrooms", "int", "bathrooms", "int"), ("bedrooms", "int", "bedrooms", "int"), ("fee", "double", "fee", "double"), ("has_photo", "boolean", "has_photo", "boolean"), ("pets_allowed", "boolean", "pets_allowed", "boolean"), ("price_type", "string", "price_type", "string"), ("square_feet", "int", "square_feet", "int"), ("address", "string", "address", "string"), ("cityname", "string", "cityname", "string"), ("state", "string", "state", "string"), ("latitude", "decimal", "latitude", "decimal"), ("longitude", "decimal", "longitude", "decimal"), ("price", "double", "price", "double"), ("price_display_new", "string", "price_display_new", "decimal")], transformation_ctx="ChangeSchema_node1750083050741")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750088564003 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750088548758, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DOUBLE PRECISION, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp DATE);"}, transformation_ctx="AmazonRedshift_node1750088564003")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750089623284 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750089607108, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.user_viewing", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewing (user_id VARCHAR, apartment_id VARCHAR, viewed_at DATE, is_wishlisted BOOLEAN, call_to_action VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750089623284")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750089168971 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750089156783, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings (booking_id VARCHAR, user_id VARCHAR, apartment_id VARCHAR, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DOUBLE PRECISION, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750089168971")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750085505661 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1750083050741, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-396468676537-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DOUBLE PRECISION, has_photo BOOLEAN, pets_allowed BOOLEAN, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL, price DOUBLE PRECISION, price_display_new DECIMAL);"}, transformation_ctx="AmazonRedshift_node1750085505661")

job.commit()