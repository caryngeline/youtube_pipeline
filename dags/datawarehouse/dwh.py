from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import parse_duration, transform_data

import logging
logger = logging.getLogger(__name__)

from airflow.decorators import task  # pyright: ignore[reportMissingImports]

table = "youtube_api"

@task
def staging_table():

    schema = 'staging'
    conn, cursor = None, None

    try: 
        # Create cursor and connection objects
        conn, cursor = get_conn_cursor()

        # Create schema and tables
        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cursor, schema)
        
        # Load data from JSON file 
        data = load_data()

        # Insert row values into table
        for row in data:
            # If the table does not have any records, insert
            if len(table_ids) == 0:
                insert_rows(conn, cursor, schema, row)

            else:
                # Check if the video_id exists, if it does, update, else insert
                if row["video_id"] in table_ids:
                    update_rows(conn, cursor, schema, row)
                else:
                    insert_rows(conn, cursor, schema, row)

        # Create a set which contains the video ids in the table
        ids_in_json = {row["video_id"] for row in data}
        
        # Define ids to be deleted as the diffference between the video ids in staging
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(conn, cursor, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e

    finally:
        # Check if the connection and cursor objects are closed
        if conn and cursor:
            close_conn_cursor(conn, cursor)


@task
def core_table():

    schema = 'core'
    conn, cursor = None, None

    try:
        # Create cursor and connection objects
        conn, cursor = get_conn_cursor()

        # Create schema and tables
        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cursor, schema)
        current_video_ids = set()

        # Load data from staging table
        cursor.execute(f"SELECT * FROM staging.{table}")
        data = cursor.fetchall()

        for row in data:
            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(conn, cursor, schema, transformed_row)
            
            else:
                transformed_row = transform_data(row)
                
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(conn, cursor, schema, transformed_row)

                else: 
                    insert_rows(conn, cursor, schema, transformed_row)
        
        # Delete what is already in the table but not in the current execution
        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(conn, cursor, schema, ids_to_delete)
        
        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e
    
    finally:
        # Check if the connection and cursor objects are closed
        if conn and cursor:
            close_conn_cursor(conn, cursor)