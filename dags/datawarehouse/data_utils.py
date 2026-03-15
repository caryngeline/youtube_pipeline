from airflow.providers.postgres.hooks.postgres import PostgresHook  # pyright: ignore[reportMissingImports]
from psycopg2.extras import RealDictCursor  # pyright: ignore[reportMissingModuleSource]

table = "youtube_api"

def get_conn_cursor():
    # From AIRFLOW_CONN_POSTGRES_DB_YT_ELT in yaml, ELT_DATABASE_NAME in .env
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    return conn, cursor
    # Query will look like this
    # cursor.execute("SELECT * FROM table")

def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close()

def create_schema(schema):
    # Open connection
    conn, cursor = get_conn_cursor()

    # Create a SCHEMA using SQL, execute SQL
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}; "
    cursor.execute(schema_sql)

    # Commit
    conn.commit()

    # Close connection
    close_conn_cursor(conn, cursor)

def create_table(schema):
    conn, cursor = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );"""
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );"""

    cursor.execute(table_sql)
    conn.commit()

    close_conn_cursor(conn, cursor)

def get_video_ids(cursor, schema):
    cursor.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cursor.fetchall()

    # Example output
    # [{"Video_ID" : "abc123"}, {"Video_ID" : "def456"}, {"Video_ID" : "ghi789"}]

    video_ids = [row["Video_ID"] for row in ids]

    return video_ids


