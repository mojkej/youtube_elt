from unittest import mock

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
