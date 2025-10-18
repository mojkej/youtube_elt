import os
from unittest import mock

import psycopg2
import pytest
from airflow.models import Connection, DagBag, Variable


@pytest.fixture
def api_key():
    """Fixture to set up API keys as Airflow Variables for testing."""
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="api_key_123"):
        yield Variable.get("API_KEY", "api_key_123")


@pytest.fixture
def channel_handle():
    """Fixture to set up Channel Handle as Airflow Variable for testing."""
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="channel_handle_123"):
        yield Variable.get("CHANNEL_HANDLE", "channel_handle_123")


@pytest.fixture
def max_results():
    """Fixture to set up Max Results as Airflow Variable for testing."""
    with mock.patch.dict("os.environ", AIRFLOW_VAR_MAX_RESULTS="10"):
        yield Variable.get("MAX_RESULTS", "10")


@pytest.fixture
def mock_requests_get():
    with mock.patch("requests.get") as mock_get:
        yield mock_get


@pytest.fixture
def mock_soda_scan():
    with mock.patch("airflow.operators.bash.BashOperator") as mock_bash_operator:
        yield mock_bash_operator


@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock-username",
        host="mock-host",
        port=6234,
        schema="mock-schema",
        password="mock-password",
    )
    conn_uri = conn.get_uri()
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")


@pytest.fixture()
def dagbag():
    yield DagBag()


@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(var_name):
        env_var = f"AIRFLOW_VAR_{var_name.upper()}"
        return os.environ.get(env_var)

    return get_airflow_variable


@pytest.fixture()
def postgres_connection():
    dbname = os.environ.get("ELT_DATABASE_NAME")
    port = os.environ.get("POSTGRES_CONN_PORT")
    user = os.environ.get("ELT_DATABASE_USERNAME")
    host = os.environ.get("POSTGRES_CONN_HOST")
    password = os.environ.get("ELT_DATABASE_PASSWORD")
    connection = None
    try:
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        yield connection
    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to PostgreSQL database: {e}")
    finally:
        if connection:
            connection.close()
