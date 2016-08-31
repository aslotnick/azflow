from azflow.Task import Task

class BashOperator(Task):
    def __init__(self, task_id, dag, bash_command):
        Task.__init__(self, task_id, dag)
        self.bash_command = bash_command
        