from azflow.DAG import DAG
from azflow.BashOperator import BashOperator

this_dag = DAG(dag_id='this_dag')

task_1 = BashOperator(task_id='task_1', dag=this_dag, 
                      bash_command='echo "task 1"')

task_2a = BashOperator(task_id='task_2a', dag=this_dag, 
                       bash_command='echo "task 2a"')
task_2a.set_upstream(task_1)

task_2b = BashOperator(task_id='task_2b', dag=this_dag, 
                       bash_command='echo "task 2b"')
task_2b.set_upstream(task_1)

task_3 = BashOperator(task_id='task_3', dag=this_dag, 
                      bash_command='echo "task 3"')
task_3.set_upstream(task_2a)
task_3.set_upstream(task_2b)

this_dag.print_tasks()
this_dag.render_tasks('example/example_flow')
