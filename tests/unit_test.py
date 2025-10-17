def test_api_key(api_key):
    assert api_key == "api_key_123"


def test_channel_handle(channel_handle):
    assert channel_handle == "channel_handle_123"


def test_max_results(max_results):
    assert max_results == "10"


def test_mock_requests_get(mock_requests_get):
    mock_requests_get.return_value.status_code = 200
    response = mock_requests_get("http://example.com")
    assert response.status_code == 200


def test_postgres_conn_vars(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock-username"
    assert conn.host == "mock-host"
    assert conn.port == 6234
    assert conn.schema == "mock-schema"
    assert conn.password == "mock-password"


def test_dagbag(dagbag):
    # Check if DAGs are loaded correctly
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    # To Verify specific DAGs are present
    expected_ids = ["produce_csv_youtube", "update_db", "data_quality_checks"]
    dag_ids = list(dagbag.dags.keys())
    print(dagbag.dags.keys())
    # To Check if the expected task count matches the actual task count
    expected_task_count = {
        "produce_csv_youtube": 4,
        "update_db": 2,
        "data_quality_checks": 2,
    }
    # Check if the expected task count matches the actual task count
    assert dagbag.size() == len(dag_ids)
    # Check for expected DAG IDs

    # Verify task counts for each DAG
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_count[dag_id]
        actual_count = len(dag.tasks)
        assert expected_count == actual_count, f"Task count mismatch for DAG {dag_id}: expected {expected_count}, got {actual_count}"

    for dag_id in expected_ids:
        assert dag_id in dag_ids, f"DAG {dag_id} not found in DagBag"
