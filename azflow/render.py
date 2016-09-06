import argparse
from azflow.DAG import DAG


def get_dags(filename):
    """
    Open filename and execute its code.
    Return any DAG instances that were loaded.
    """
    with open(filename) as f:
        code = f.read()
        exec(code)
    dags = [d[1] for d in locals().items() if isinstance(d[1],DAG)]
    return dags


def process_file(filename, output_folder, print_dag=False):
    dags = get_dags(filename)
    for dag in dags:
        print('Rendering {}'.format(dag.dag_id))
        dag.render_tasks(output_folder)
        if print_dag:
            dag.print_tasks()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Render a DAG as .job files '+
                                    'for an Azkaban flow')   
    parser.add_argument('--dag_file', required=True, 
                        help='file containing a single DAG object')
    parser.add_argument('--output_folder', required=True, 
                        help='folder name to save .job files')
    parser.add_argument('--print_dag', action='store_true',
                        help='Print the DAG while rendering')

    args = parser.parse_args()
    process_file(args.dag_file, args.output_folder, args.print_dag)