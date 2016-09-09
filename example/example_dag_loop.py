from azflow.DAG import DAG
from azflow.BashOperator import BashOperator

loop_dag = DAG(dag_id='loop_dag')

task_1 = BashOperator(task_id='task_1', dag=loop_dag, 
                      bash_command='echo "begin"')

task_3 = BashOperator(task_id='task_3', dag=loop_dag, 
                      bash_command='echo "clean up"')

tasks_to_loop = ['do', 'all', 'these', 'in', 'no','particular', 'order']
for t in tasks_to_loop:
    task_2a = BashOperator(task_id=t+'_part_1', dag=loop_dag, 
                       bash_command='echo "start {}"'.format(t))
    task_2a.set_upstream(task_1)

    task_2b = BashOperator(task_id=t+'_part_2', dag=loop_dag, 
                       bash_command='echo "finish {}"'.format(t))
    task_2b.set_upstream(task_2a)
    task_3.set_upstream(task_2b)