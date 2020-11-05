import datetime

from airflow import configuration
from airflow.models import DagBag, TaskInstance

dag_bag = DagBag(configuration.get('core', 'dags_folder'))
dag_list = dag_bag.dags

bad_dag_ids = []
bad_task_ids = {}
bad_task_id_counter = 0

for dag_id in dag_list:
    dag = dag_bag.dags[dag_id]
    print(f'Checking {dag_id} DAG...')

    if len(dag_id) > 63:
        bad_dag_ids.append(dag_id)

    for task in dag.tasks:
        ti = TaskInstance(task, datetime.datetime.now())
        if len(ti.task_id) > 63:
            if dag_id not in bad_task_ids:
                bad_task_ids[dag_id] = []
            bad_task_ids[dag_id].append(ti.task_id)
            bad_task_id_counter += 1

print()

if len(bad_dag_ids) > 0 or bad_task_id_counter > 0:
    print("dag_id greater than 63 characters:")
    print("\n".join(bad_dag_ids))
    print()
    print("task_id greater than 63 characters:")
    for dag_id, task_ids in bad_task_ids.items():
        print(f'{dag_id} {task_ids}')
        print()
        print('task_id/dag_id are not safe to use as Kubernetes labels. This can cause '
              'severe performance regressions. '
              'Please keep task_id/dag_id length below 63 characters. '
              'Please see '
              '<https://kubernetes.io/docs/concepts/overview/working-with-objects'
              '/labels/#syntax-and-character-set>. ')
    exit(1)
