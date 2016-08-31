from os import mkdir
from azflow.Task import Task

class DAG():
    """ Define a directed acyclic graph of tasks.
        Render them as Azkaban  jobs.
        Create a "noop" task to act as the head of the graph ("flow" in Azkaban)
    """
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.tasks = set()
        self.flow_task = Task(task_id=self.dag_id, dag=self)


    def add_task(self, task):
        self.tasks.add(task)


    def render_tasks(self, output_directory):
        """ Output the necessary .job files to express this DAG 
            as an Azkaban job flow.
            Any tasks that have no dependencies will depend on the 
            dag's self.flow_task
        """
        try:
            mkdir(output_directory)
        except FileExistsError:
            pass
        for t in self.tasks:
            if not t is self.flow_task and not t.dependencies:
                t.set_upstream(self.flow_task)
            task_path = output_directory+'/'+t.task_id+'.job'
            with open(task_path, 'w') as f:
                f.write(t.render())
