import pandas as pd
import numpy as np
import logging
import boto3
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_columns(df, required_cols):
    """
    Validates if all required columns are present in the DataFrame.
    Raises a ValueError if any column is missing.
    """
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Required columns missing: {missing_cols}")
    return True

def transformation_logic(streams_df, users_df, songs_df):
    """
    Applies transformation logic to merge and enrich stream data.
    Ensures data types are consistent and necessary derived columns are created.
    """
    # Ensure ID columns are string type for consistent merging
    streams_df['user_id'] = streams_df['user_id'].astype(str)
    streams_df['track_id'] = streams_df['track_id'].astype(str)
    users_df['user_id'] = users_df['user_id'].astype(str)
    songs_df['track_id'] = songs_df['track_id'].astype(str)

    # Convert listen_time to datetime and extract hour
    streams_df['listen_time'] = pd.to_datetime(streams_df['listen_time'], utc=True)
    streams_df['listen_hour'] = streams_df['listen_time'].dt.hour

    # Merge stream data with user and song metadata
    merged_df = streams_df.merge(users_df, on='user_id', how='left')
    merged_df = merged_df.merge(songs_df, on='track_id', how='left')

    # Calculate listen counts per user and per track (for later KPI derivation)
    merged_df['listen_count_user'] = merged_df.groupby('user_id')['track_id'].transform('count')
    merged_df['listen_count_track'] = merged_df.groupby('track_id')['user_id'].transform('count')

    # Calculate popularity_index - NOW INCLUDED IN THE OUTPUT CSV
    merged_df['popularity_index'] = (
        merged_df['listen_count_track'].fillna(0) * 0.6 +
        merged_df['popularity'].fillna(0) * 0.4
    )

    # Ensure numeric columns are handled, filling NaNs before converting to int
    numeric_int_cols = [
        'user_age', 'popularity', 'duration_ms', 'key', 'mode', 'time_signature',
        'listen_count_user', 'listen_count_track', 'listen_hour' # listen_hour also an int
    ]
    for col in numeric_int_cols:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].fillna(0).astype('Int64')

    # Truncate artists string to match Redshift VARCHAR(500)
    merged_df['artists'] = merged_df['artists'].astype(str).str[:500]

    # Ensure explicit is boolean
    merged_df['explicit'] = merged_df['explicit'].astype(bool)

    # Define the final columns and their order to match stg_streaming table in Redshift
    final_cols = [
        'user_id', 'track_id', 'listen_time', 'listen_hour', 'user_name',
        'user_age', 'user_country', 'created_at', 'id', 'artists',
        'album_name', 'track_name', 'popularity', 'duration_ms', 'explicit',
        'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
        'time_signature', 'track_genre', 'listen_count_user', 'listen_count_track',
        'popularity_index'
    ]

    # Select and reorder columns, dropping any that are not in final_cols
    merged_df = merged_df[final_cols]

    return merged_df

def transform_data(bucket, **kwargs):
    """
    Airflow callable for the transformation task.
    Loads raw data, applies transformations, and uploads the transformed data to S3.
    """
    try:
        ti = kwargs['ti']
        # Pull unprocessed stream keys from the extraction task
        unprocessed_streams = ti.xcom_pull(task_ids='extract_data', key='unprocessed_streams')

        if not unprocessed_streams:
            logger.warning("No unprocessed stream files found by extract_data task. Skipping transformation.")
            # Push empty values to XCom so downstream tasks don't fail looking for data
            ti.xcom_push(key='temp_key', value=None)
            ti.xcom_push(key='unprocessed_streams_for_load', value=[]) # Pass empty list for loading
            logger.debug(f"DEBUG (transformation): Pushing empty unprocessed_streams_for_load: []")
            return None

        s3 = boto3.client('s3')

        def load_csv(key):
            """Helper function to load a CSV file from S3 into a Pandas DataFrame."""
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj['Body'])

        # Process the first unprocessed stream file.
        current_stream_key = unprocessed_streams[0]
        streams_df = load_csv(current_stream_key)
        users_df = load_csv('raw/metadata/users.csv')
        songs_df = load_csv('raw/metadata/songs.csv')

        # Validate essential columns before proceeding
        validate_columns(streams_df, ['user_id', 'track_id', 'listen_time'])
        validate_columns(users_df, ['user_id', 'user_age', 'user_country', 'created_at'])
        validate_columns(songs_df, ['track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre', 'id'])

        # Apply the main transformation logic
        merged_df = transformation_logic(streams_df, users_df, songs_df)

        # Generate a unique temporary key for the transformed data in S3
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        temp_key = f'temp/transformed_data_{timestamp}.csv'

        # Save the transformed DataFrame to a temporary CSV file locally
        local_temp_file_path = '/tmp/transformed_data.csv'
        merged_df.to_csv(local_temp_file_path, index=False)
        logger.info(f"Transformed data saved locally to {local_temp_file_path}")

        # Upload the transformed CSV to S3
        s3.upload_file(local_temp_file_path, bucket, temp_key)
        logger.info(f"Transformed data uploaded to s3://{bucket}/{temp_key}")

        # Push the temporary key and the list of unprocessed streams (for the loader to move) to XCom
        ti.xcom_push(key='temp_key', value=temp_key)
        ti.xcom_push(key='unprocessed_streams_for_load', value=[current_stream_key]) # Only pass the currently processed file for moving
        logger.debug(f"DEBUG (transformation): Pushing unprocessed_streams_for_load: {[current_stream_key]}")

        return temp_key
    except Exception as e:
        logger.error(f"Error in transformation: {e}")
        raise

