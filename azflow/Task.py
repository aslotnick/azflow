from azflow.AzflowException import AzflowException

class Task():
    def __init__(self, task_id, dag):
        self.dependencies = set()
        self.job_type = 'noop'
        self.task_id = task_id 
        dag.add_task(self)


    def set_upstream(self, upstream_task):
        if upstream_task is self:
            raise AzflowException('circular dependency')
        self.dependencies.add(upstream_task)


    def render(self):
        rendered = 'type={}\n'.format(self.job_type)
        if self.dependencies:
            rendered += 'dependencies=' + ','.join(
                        [t.task_id + '.job' for t in self.dependencies]) + '\n'
        return rendered



    