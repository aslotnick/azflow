from os import mkdir
from azflow.Task import Task
from azflow.AzflowException import AzflowException
from collections import deque

class DAG():
    """ Define a directed acyclic graph of tasks.
        Render tasks as Azkaban  jobs, with a final 
        task representing the DAG as an Azkaban "flow".
    """
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.tasks = set()
        self.flow_task = Task(task_id=self.dag_id, dag=self)


    def add_task(self, task):
        self.tasks.add(task)


    def _prepare_flow(self):
        """The flow_task should come at the end of the DAG,
            so it will depend on all tasks which no other task depends on.
        """
        for t in self.tasks:
            if not t is self.flow_task and not t.downstream:
                self.flow_task.set_upstream(t)


    def _validate(self):
        """ Validate that DAG is acyclic and that the flow_task is last"""

        def _contains_cycle(head, ancestors):
            """ Perform a DFS, starting from the flow_task and working upstream.
            If at any point a node is its own ancestor,
            return True.
            """
            if head in ancestors:
                return  True
            if head.upstream:
                for node in head.upstream:
                    result = _contains_cycle(node, ancestors+[head])
                    if result:
                        return True
            return False

        if _contains_cycle(self.flow_task, []):
            raise AzflowException('Invalid DAG - cycle detected')
        if not self.flow_task.upstream:
            raise AzflowException('Invalid flow - flow_task detached')



    def render_tasks(self, output_directory):
        """ Output the necessary .job files to express this DAG 
            as an Azkaban job flow.
        """
        self._prepare_flow()
        self._validate()
        try:
            mkdir(output_directory)
        except FileExistsError:
            pass
        for t in self.tasks:
            task_path = output_directory+'/'+t.task_id+'.job'
            with open(task_path, 'w') as f:
                f.write(t.render())

    def print_tasks(self):
        """ Use BFS """
        self._prepare_flow()
        self._validate() 

        def bfs(node):
            to_visit = deque() # queue
            visited = set()
            to_visit.append((node,0))
            to_print = deque() # stack
            while to_visit:
                current, num_ancestors = to_visit.popleft()
                if current not in visited:
                    to_print.appendleft((current.task_id, num_ancestors))
                    visited.add(current)
                to_visit.extend([(n, num_ancestors+1) 
                                        for n in current.upstream 
                                            if n not in visited])
            max_ancestors = to_print[0][1]
            for task_id, num_ancestors in to_print:
                print((max_ancestors-num_ancestors)*'<-'+task_id)

        bfs(self.flow_task)



