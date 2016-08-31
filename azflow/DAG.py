from os import mkdir

class DAG():
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.tasks = set()

    def add_task(self, task):
        self.tasks.add(task)

    def render_tasks(self, output_directory):
        try:
            mkdir(output_directory)
            print('a')
        except FileExistsError:
            pass
        for t in self.tasks:
            with open(t.task_id+'.job', 'w') as f:
                f.write(t.render())

