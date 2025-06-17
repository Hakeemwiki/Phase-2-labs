
import pandas as pd
import boto3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data(bucket, prefix='raw/streams/', **kwargs):
    """
    Extracts unprocessed stream file keys from the specified S3 bucket.

    This function lists objects in the S3 bucket under the 'raw/streams/' prefix,
    filters out any files that have already been moved to a 'processed/' subfolder,
    and pushes the keys of unprocessed stream files to Airflow's XCom.

    If no unprocessed files are found, it logs a warning and returns an empty list,
    allowing the DAG to continue gracefully without failure.

    Args:
        bucket (str): The name of the S3 bucket to extract data from.
        prefix (str): The S3 prefix where raw stream files are located.
        **kwargs: Airflow context arguments, including 'ti' (TaskInstance) for XCom.

    Returns:
        list: A list of S3 keys (paths) to the unprocessed stream files.
    """
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Filter for .csv files that are not already in a 'processed/' subfolder
        stream_keys = [
            item['Key'] for item in response.get('Contents', [])
            if item['Key'].endswith('.csv') and 'processed/' not in item['Key']
        ]

        if not stream_keys:
            logger.warning("No unprocessed stream files found in S3 for extraction.")
            # Return an empty list to XCom instead of raising an error
            ti = kwargs['ti']
            ti.xcom_push(key='unprocessed_streams', value=[])
            return []

        # Push keys to XCom for downstream tasks
        ti = kwargs['ti']
        ti.xcom_push(key='unprocessed_streams', value=stream_keys)
        logger.info(f"Found {len(stream_keys)} unprocessed stream file(s): {stream_keys}")
        return stream_keys
    except Exception as e:
        logger.error(f"Error in extraction: {e}")
        raise # Re-raise exception for other types of errors (e.g., S3 access issues)


