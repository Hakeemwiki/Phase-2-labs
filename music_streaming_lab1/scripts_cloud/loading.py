
import pandas as pd
import boto3
import json
import logging
import os
from sqlalchemy import create_engine

# Configure logging for better visibility in Airflow logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data(**kwargs):
    """
    Airflow callable for the loading task using Incremental Aggregation.
    This function performs the following steps:
    1. Pulls transformed data's S3 key and the list of unprocessed stream keys from XCom.
    2. Establishes a connection to the Redshift cluster using credentials from AWS Secrets Manager.
    3. Defines and ensures the existence of necessary Redshift tables:
        - `stg_streaming`: A temporary staging table for the newly transformed data.
        - `fact_raw_streams`: A permanent fact table to store all historical transformed stream events.
        - `fact_streaming_kpis`: The main fact table for aggregated KPIs.
        - `stg_affected_kpis`: A temporary table to hold re-calculated KPIs for affected dimensions.
    4. Copies the new transformed data from S3 into `stg_streaming`.
    5. Performs an upsert operation (DELETE then INSERT) to merge the new data from `stg_streaming`
       into `fact_raw_streams`, ensuring uniqueness for stream events.
    6. Crucially, calculates KPIs incrementally**: It identifies the `track_genre` and `listen_hour`
       combinations present in the newly loaded `stg_streaming` data. It then recalculates
       all KPIs for these specific combinations by querying the *entire* `fact_raw_streams` table,
       storing the results in `stg_affected_kpis`.
    7. Performs an upsert (DELETE then INSERT) from `stg_affected_kpis` into `fact_streaming_kpis`,
       updating only the affected KPI rows.
    8. Moves the original processed stream file(s) from the 'raw/streams' folder to the
       'raw/streams/processed' folder in S3, marking them as handled.

    Args:
        **kwargs: Airflow context arguments, including 'ti' (TaskInstance) for XCom
                  and 'dag_run' for dynamic configurations like 'bucket'.
    """
    try:
        ti = kwargs['ti']
        # Pull necessary information from XCom, pushed by previous tasks
        temp_key = ti.xcom_pull(task_ids='transform_data', key='temp_key')
        unprocessed_streams_for_load = ti.xcom_pull(task_ids='transform_data', key='unprocessed_streams_for_load')
        # Get S3 bucket name from DAG run configuration or use a default
        bucket = kwargs['dag_run'].conf.get('bucket', 'my-etl-bucket-20250611')

        # If no transformed data was produced (e.g., no new raw files), skip loading
        if not temp_key:
            logger.info("No transformed data found from transformation task. Skipping load into Redshift.")
            return

        # Initialize S3 client for interacting with S3 bucket
        s3 = boto3.client('s3')
        # Initialize Secrets Manager client and retrieve Redshift credentials
        secrets = boto3.client('secretsmanager')
        secret = secrets.get_secret_value(SecretId='arn:aws:secretsmanager:eu-north-1:396468676537:secret:redshift-secret-complete-20250612-7qboMC')
        creds = json.loads(secret['SecretString'])

        # Create SQLAlchemy engine for connecting to Redshift
        engine = create_engine(
            f"postgresql+psycopg2://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}"
        )

        # Define Redshift table schemas using DDL statements
        # stg_streaming: Temporary staging table to hold the current batch of transformed data
        # DROP and CREATE ensures a clean table for each load operation.
        schema_sql = """
        DROP TABLE IF EXISTS stg_streaming;
        CREATE TABLE stg_streaming (
            user_id              INT,
            track_id             VARCHAR(100),
            listen_time          TIMESTAMPTZ,
            listen_hour          INT,
            user_name            VARCHAR(100),
            user_age             INT,
            user_country         VARCHAR(100),
            created_at           TIMESTAMPTZ,
            id                   INT,
            artists              VARCHAR(500),
            album_name           VARCHAR(500),
            track_name           VARCHAR(500),
            popularity           INT,
            duration_ms          INT,
            explicit             BOOLEAN,
            danceability         FLOAT4,
            energy               FLOAT4,
            key                  INT,
            loudness             FLOAT4,
            mode                 INT,
            speechiness          FLOAT4,
            acousticness         FLOAT4,
            instrumentalness     FLOAT4,
            liveness             FLOAT4,
            valence              FLOAT4,
            tempo                FLOAT4,
            time_signature       INT,
            track_genre          VARCHAR(100),
            listen_count_user    INT,
            listen_count_track   INT,
            popularity_index     FLOAT4
        );

        -- fact_raw_streams: Permanent fact table to store all historical transformed stream events.
        -- Explicitly DROP and CREATE to ensure schema changes are applied.
        DROP TABLE IF EXISTS fact_raw_streams;
        CREATE TABLE fact_raw_streams (
            user_id              INT,
            track_id             VARCHAR(100),
            listen_time          TIMESTAMPTZ,
            listen_hour          INT,
            user_name            VARCHAR(100),
            user_age             INT,
            user_country         VARCHAR(100),
            created_at           TIMESTAMPTZ,
            id                   INT,
            artists              VARCHAR(500),
            album_name           VARCHAR(500),
            track_name           VARCHAR(500),
            popularity           INT,
            duration_ms          INT,
            explicit             BOOLEAN,
            danceability         FLOAT4,
            energy               FLOAT4,
            key                  INT,
            loudness             FLOAT4,
            mode                 INT,
            speechiness          FLOAT4,
            acousticness         FLOAT4,
            instrumentalness     FLOAT4,
            liveness             FLOAT4,
            valence              FLOAT4,
            tempo                FLOAT4,
            time_signature       INT,
            track_genre          VARCHAR(100),
            listen_count_user    INT,
            listen_count_track   INT,
            popularity_index     FLOAT4
        );

        -- fact_streaming_kpis: Main aggregated KPI table.
        -- Explicitly DROP and CREATE to ensure schema changes are applied.
        DROP TABLE IF EXISTS fact_streaming_kpis;
        CREATE TABLE fact_streaming_kpis (
            track_genre VARCHAR(100) SORTKEY,
            listen_count BIGINT,
            avg_duration_ms FLOAT,
            popularity_index FLOAT,
            most_popular_track VARCHAR(1000),
            most_popular_artist VARCHAR(1000),
            listen_hour INTEGER DISTKEY,
            unique_listeners INTEGER,
            top_artist VARCHAR(1000),
            track_diversity_index FLOAT
        );

        -- stg_affected_kpis: Temporary table to hold the re-calculated KPIs for only
        -- the genre/hour combinations that are affected by the current load.
        -- It has the same structure as fact_streaming_kpis.
        DROP TABLE IF EXISTS stg_affected_kpis;
        CREATE TEMPORARY TABLE stg_affected_kpis (LIKE fact_streaming_kpis);
        """

        # Execute the DDL statements to ensure tables exist and staging is clean
        with engine.begin() as conn:
            conn.execute(schema_sql)
        logger.info("Redshift schemas for stg_streaming, fact_raw_streams, fact_streaming_kpis, and stg_affected_kpis ensured.")

        # Copy the newly transformed data from S3 into the temporary stg_streaming table
        copy_sql_stg_streaming = f"""
        COPY stg_streaming FROM 's3://{bucket}/{temp_key}'
        IAM_ROLE 'arn:aws:iam::396468676537:role/service-role/AmazonRedshift-CommandsAccessRole-20250604T113408'
        FORMAT AS CSV IGNOREHEADER 1 TIMEFORMAT 'auto'
        NULL AS '';        -- Treat empty strings in CSV as NULL values in Redshift
        """
        with engine.begin() as conn:
            conn.execute(copy_sql_stg_streaming)
        logger.info(f"Data from s3://{bucket}/{temp_key} copied into stg_streaming.")

        # Upsert new records from stg_streaming into fact_raw_streams.
        # This handles cases where the same source file might be processed again (e.g., during testing or retry)
        # by deleting existing matching records and then inserting the new ones.
        # It assumes (user_id, track_id, listen_time) forms a unique composite key for a stream event.
        upsert_raw_streams_sql = """
        BEGIN;
        -- Delete any records in fact_raw_streams that already exist in the current stg_streaming batch
        DELETE FROM fact_raw_streams
        USING stg_streaming
        WHERE
            fact_raw_streams.user_id = stg_streaming.user_id AND
            fact_raw_streams.track_id = stg_streaming.track_id AND
            fact_raw_streams.listen_time = stg_streaming.listen_time;

        -- Insert all records from stg_streaming into fact_raw_streams
        INSERT INTO fact_raw_streams (
            user_id, track_id, listen_time, listen_hour, user_name, user_age, user_country,
            created_at, id, artists, album_name, track_name, popularity, duration_ms, explicit,
            danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness,
            liveness, valence, tempo, time_signature, track_genre, listen_count_user, listen_count_track,
            popularity_index
        )
        SELECT
            user_id, track_id, listen_time, listen_hour, user_name, user_age, user_country,
            created_at, id, artists, album_name, track_name, popularity, duration_ms, explicit,
            danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness,
            liveness, valence, tempo, time_signature, track_genre, listen_count_user, listen_count_track,
            popularity_index
        FROM stg_streaming;
        COMMIT;
        """
        with engine.begin() as conn:
            conn.execute(upsert_raw_streams_sql)
        logger.info("New stream data upserted into fact_raw_streams (historical data).")

        # --- Incremental Aggregation of KPIs ---
        # 1. Recalculate KPIs from 'fact_raw_streams' for only the 'track_genre' and 'listen_hour'
        #    combinations that are present in the 'stg_streaming' (newly loaded) data.
        # 2. Store these re-calculated (and cumulative for affected dimensions) KPIs in 'stg_affected_kpis'.
        # 3. Perform an upsert (DELETE then INSERT) on 'fact_streaming_kpis' using 'stg_affected_kpis'.
        incremental_kpi_sql = """
        BEGIN;
        -- Step 1: Populate stg_affected_kpis with re-calculated KPIs for affected genre/hour combinations.
        INSERT INTO stg_affected_kpis (
            track_genre, listen_count, avg_duration_ms, popularity_index,
            most_popular_track, most_popular_artist, listen_hour, unique_listeners,
            top_artist, track_diversity_index
        )
        SELECT
            agg.track_genre,
            agg.listen_count,
            agg.avg_duration_ms,
            (agg.listen_count * 0.6 + agg.avg_popularity * 0.4) AS popularity_index,
            MAX(CASE WHEN rt.rn_pop = 1 THEN rt.track_name ELSE NULL END) AS most_popular_track,
            MAX(CASE WHEN rt.rn_pop = 1 THEN rt.artists ELSE NULL END) AS most_popular_artist,
            agg.listen_hour,
            agg.unique_listeners,
            MAX(CASE WHEN ra.rn_artist_total_listens = 1 THEN ra.artists ELSE NULL END) AS top_artist, -- Get top_artist from the new CTE
            agg.track_diversity_index
        FROM (
            -- Base aggregation for most metrics, focusing only on affected genre/hour combinations
            SELECT
                track_genre,
                listen_hour,
                SUM(listen_count_track) AS listen_count,
                AVG(duration_ms) AS avg_duration_ms,
                AVG(popularity) AS avg_popularity,
                COUNT(DISTINCT user_id) AS unique_listeners,
                (1 - (COUNT(DISTINCT track_id)::FLOAT / NULLIF(SUM(listen_count_track), 0)::FLOAT)) AS track_diversity_index
            FROM fact_raw_streams
            WHERE (track_genre, listen_hour) IN (SELECT DISTINCT track_genre, listen_hour FROM stg_streaming)
            GROUP BY track_genre, listen_hour
        ) AS agg
        LEFT JOIN (
            -- Subquery to find the most popular track for each affected genre-hour
            SELECT
                track_genre,
                listen_hour,
                track_name,
                artists,
                ROW_NUMBER() OVER (PARTITION BY track_genre, listen_hour ORDER BY popularity DESC, listen_count_track DESC) as rn_pop
            FROM fact_raw_streams
            WHERE (track_genre, listen_hour) IN (SELECT DISTINCT track_genre, listen_hour FROM stg_streaming)
        ) AS rt
            ON agg.track_genre = rt.track_genre AND agg.listen_hour = rt.listen_hour
        LEFT JOIN (
            -- Subquery to find the top artist by total listens for each affected genre-hour
            SELECT
                track_genre,
                listen_hour,
                artists,
                ROW_NUMBER() OVER (PARTITION BY track_genre, listen_hour ORDER BY SUM(listen_count_track) DESC, artists) as rn_artist_total_listens
            FROM fact_raw_streams
            WHERE (track_genre, listen_hour) IN (SELECT DISTINCT track_genre, listen_hour FROM stg_streaming)
            GROUP BY track_genre, listen_hour, artists
        ) AS ra
            ON agg.track_genre = ra.track_genre AND agg.listen_hour = ra.listen_hour
        GROUP BY
            agg.track_genre,
            agg.listen_count,
            agg.avg_duration_ms,
            agg.avg_popularity,
            agg.listen_hour,
            agg.unique_listeners,
            agg.track_diversity_index;

        -- Step 2: Delete old KPI records from fact_streaming_kpis for the affected genre/hour combinations.
        DELETE FROM fact_streaming_kpis
        USING stg_affected_kpis sak
        WHERE
            fact_streaming_kpis.track_genre = sak.track_genre AND
            fact_streaming_kpis.listen_hour = sak.listen_hour;

        -- Step 3: Insert the newly calculated (and cumulative for affected) KPIs from stg_affected_kpis
        -- into fact_streaming_kpis.
        INSERT INTO fact_streaming_kpis (
            track_genre, listen_count, avg_duration_ms, popularity_index,
            most_popular_track, most_popular_artist, listen_hour, unique_listeners,
            top_artist, track_diversity_index
        )
        SELECT * FROM stg_affected_kpis;
        COMMIT;
        """

        with engine.begin() as conn:
            conn.execute(incremental_kpi_sql)
        logger.info("KPIs incrementally aggregated and updated in fact_streaming_kpis.")

        # --- Move processed files to S3 'processed' folder ---
        # This block is now inside the try-except, ensuring it only runs on success
        if unprocessed_streams_for_load: # Ensure the list is not empty
            for stream_key in unprocessed_streams_for_load:
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': stream_key},
                    Key=f'raw/streams/processed/{os.path.basename(stream_key)}'
                )
                s3.delete_object(Bucket=bucket, Key=stream_key)
                logger.info(f"Moved processed file {stream_key} to processed/ folder.")
        else:
            logger.info("No files to move to processed folder (no new streams were processed).")

    except Exception as e:
        logger.error(f"Error in loading (Incremental Aggregation): {e}")
        # Re-raise the exception so Airflow can mark the task as failed
        raise
