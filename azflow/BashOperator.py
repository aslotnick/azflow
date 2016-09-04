from azflow.Task import Task

class BashOperator(Task):
    def __init__(self, task_id, dag, bash_command):
        Task.__init__(self, task_id, dag)
        self.bash_command = bash_command
        self.job_type = 'command'
        

    def render(self):
        rendered = Task.render(self)
        rendered += 'command=' + self.bash_command + '\n'
        return rendered
