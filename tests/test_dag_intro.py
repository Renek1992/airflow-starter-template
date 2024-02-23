
import pytest
from airflow.models import DagBag
from airflow.operators.bash import BashOperator

@pytest.fixture()
def dag_bag():
    return DagBag(dag_folder='dags/', include_examples=False)


def test_dag_loaded(dag_bag):
    """Test DAG is loaded."""
    dag_1 = dag_bag.get_dag(dag_id='dag_intro')
    assert dag_bag.import_errors == {}
    assert dag_1 is not None
    assert len(dag_1.tasks) == 4


def test_dag_operators(dag_bag):
    """Test operators in DAG."""
    dag_1 = dag_bag.get_dag(dag_id='dag_intro')
    all_tasks = dag_1.tasks

    for task in all_tasks:
        assert task.operator_class == BashOperator
    

def test_dag_structure(dag_bag):
    """Test task dependencies in DAG."""
    source_1 = {
        'print_date': ['templated', 'sleep'], 
        'sleep': ['hello_world'], 
        'hello_world': [], 
        'templated': ['hello_world']
    }
    dag_1 = dag_bag.get_dag(dag_id='dag_intro')
    assert dag_1.task_dict.keys() == source_1.keys()
    for task_id, downstream_list in source_1.items():
        assert dag_1.has_task(task_id)
        task = dag_1.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)
