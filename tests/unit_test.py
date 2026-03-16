# Naming rules for python tests: test_xx or xx_test
# Bash command: pytest -v -s unit_test_directory -k function_name

def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "JAEMIN"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars

    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name" # Schema is the db name

def test_dags_integrity(dagbag):
    # Test DAG integrity into four parts
    # If assert statement is false, return a message showing the errors
    # All print is not shown on default: put -s in bash command after -v

    # 1. No import errors
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("=============")
    print(dagbag.import_errors)

    # 2. All expected DAGs are being loaded
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_keys = list(dagbag.dags.keys())

    print("=============")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_keys, f"DAG {dag_id} is missing"

    # 3. The number of DAGs is correct
    assert dagbag.size() == 3

    print("=============")
    print(dagbag.size())

    # 4. Each DAG has a number of tasks expected
    # Should be update after restructuring DAGs
    expected_task_count = {
        "produce_json" : 5,
        "update_db" : 3,
        "data_quality" : 2
    }

    print("=============")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_count[dag_id]
        actual_count = len(dag.tasks)
        assert expected_count == actual_count, f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id, len(dag.tasks))
