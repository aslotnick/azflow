from unittest import TestCase
from azflow.DAG import DAG
from azflow.Task import Task
from azflow.AzflowException import AzflowException


class AzflowTest(TestCase):
    def test_dag_validate(self):
    	# create a valid DAG
        test_dag = DAG(dag_id='test_dag')
        task_1 = Task(task_id='task_1', dag=test_dag)
        task_2 = Task(task_id='task_2', dag=test_dag)
        task_2.set_upstream(task_1)
        task_3 = Task(task_id='task_3', dag=test_dag)
        task_3.set_upstream(task_1) 
        task_4 = Task(task_id='task_4', dag=test_dag)
        task_4.set_upstream(task_2) 
        task_4.set_upstream(task_3) 
        test_dag._prepare_flow()
        self.assertEqual(None, test_dag._validate())
        # introduce a cycle
        task_1.set_upstream(task_4) 
        test_dag._prepare_flow()
        with self.assertRaises(AzflowException):
            test_dag._validate()

    def test_task_cycle(self):
        test_dag = DAG(dag_id='test_dag')
        task_1 = Task(task_id='task_1', dag=test_dag)
        task_2 = Task(task_id='task_2', dag=test_dag)
        task_2.set_upstream(task_1)
        #try to introduce a task that depends on a direct dependency
        with self.assertRaises(AzflowException):
            task_1.set_upstream(task_2) 

