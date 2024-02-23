
import pytest
from airflow.models import DagBag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

@pytest.fixture()
def dag_bag():
    return DagBag(dag_folder='dags/', include_examples=False)


def test_dag_loaded(dag_bag):
    """Test DAG is loaded."""
    dag_1 = dag_bag.get_dag(dag_id='dag_databricks')
    assert dag_bag.import_errors == {}
    assert dag_1 is not None
    assert len(dag_1.tasks) == 2


def test_dag_operators(dag_bag):
    """Test operators in DAG."""
    dag_1 = dag_bag.get_dag(dag_id='dag_databricks')
    all_tasks = dag_1.tasks

    for task in all_tasks:
        assert task.operator_class == DatabricksRunNowOperator


def test_dag_structure(dag_bag):
    """Test task dependencies in DAG."""
    source_1 = {
        'run_now_task1': ['run_now_task2'], 
        'run_now_task2': []
    }
    dag_1 = dag_bag.get_dag(dag_id='dag_databricks')
    assert dag_1.task_dict.keys() == source_1.keys()
    for task_id, downstream_list in source_1.items():
        assert dag_1.has_task(task_id)
        task = dag_1.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)