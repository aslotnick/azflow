from azflow.AzflowException import AzflowException

class Task():
    """ A node in a DAG.
        Maintain a set of upstream tasks as these 
        are Azkaban dependencies.
        Also maintain set of downstream tasks to
        help with rendering a flow.
    """
    def __init__(self, task_id, dag):
        self.upstream = set()
        self.downstream = set()
        self.job_type = 'noop'
        self.task_id = task_id 
        dag.add_task(self)


    def _add_edges(self, other_task, direction):
        #direction should be 'upstream' or 'downstream'
        if other_task is self:
            raise AzflowException('cannot depend on self')
        if direction == 'upstream':
            task_set = self.upstream
            complement = other_task.upstream
            opposite = 'downstream'
        if direction == 'downstream':
            task_set = self.downstream
            complement = other_task.downstream
            opposite = 'upstream'
        if self in complement:
            msg = 'Setting {} {} of {} would create circular dependency'
            raise AzflowException(msg.format(self.task_id, direction, 
                                             other_task.task_id))
        if other_task not in task_set:
            task_set.add(other_task)
            other_task._add_edges(self, opposite)


    def set_upstream(self, upstream_task):    
            self._add_edges(upstream_task, 'upstream')


    def set_downstream(self, downstream_task):
            self._add_edges(upstream_task, 'downstream')


    def render(self):
        rendered = 'type={}\n'.format(self.job_type)
        if self.upstream:
            rendered += 'dependencies=' + ','.join(
                        [t.task_id for t in self.upstream]) + '\n'
        return rendered



    