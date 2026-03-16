import os
import pytest  # pyright: ignore[reportMissingImports]
from unittest import mock
from airflow.models import Variable, Connection, DagBag  # pyright: ignore[reportMissingImports]
import psycopg2  # pyright: ignore[reportMissingModuleSource]

# Tests for variable mocking
@pytest.fixture()
def api_key():
    # Temporarily updates the environment with key-value pair
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")

@pytest.fixture()
def channel_handle():
    # Temporarily updates the environment with key-value pair
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="JAEMIN"):
        yield Variable.get("CHANNEL_HANDLE")

@pytest.fixture()
def mock_postgres_conn_vars():
    # Define a mock connection variable named conn and set to Connection class
    conn = Connection(
        login = "mock_username",
        password = "mock_password",
        host = "mock_host",
        port = 1234,
        schema = "mock_db_name" # Schema is the db name
    )

    conn_uri = conn.get_uri()
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id = "POSTGRES_DB_YT_ELT")

@pytest.fixture()
def dagbag():
    yield DagBag()

# Integration testing tests how different parts of the system work together
# Example is how we transform the data from staging into core layer and upload it to the Postgres table
# Aim to use real credentials

@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable

@pytest.fixture()
def real_postgres_connection():
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password, 
            host=host,
            port=port
        )

        yield conn

    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to database: {e}")

    finally:
        if conn:
            conn.close()