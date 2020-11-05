"""
Airflow tasks only provide an easy interface to direct dependants (upstream or downstream). Using
simple recursion we can get all direct and intermediate dependants of a given task or set of tasks.

This is intended to be used in conjunction with `group_airflow_tasks` to ease defintions between
not connected groups of tasks. This is helpful in situations where not all tasks are available in
the DAG defintion file (because sub-tasks are defined in a sub-module for example).

For example
```python
(start_datahub_import, finish_datahub_import,) = group_airflow_tasks(
    get_all_airflow_dependants(datahub_import_head_task, upstream=False),
    group_name="datahub_import",
)
```

Use with care: If your DAG is already fully connected, calling `get_all_airflow_dependants` will
yield the entire (upstream or downstream) DAG!
"""
from typing import List, Set, Union

# from airflow.models.base import Operator  # replace in Airflow 2.0
from airflow.models.baseoperator import BaseOperator as Operator


def recur_airflow_dependants(
    task: Operator,
    dependants: Set[Operator] = set(),
    upstream: bool = False,
) -> Set[Operator]:
    """
    Add my self to the set of dependants and recur over all other dependants.
    """
    dependants.update([task])

    if task.get_direct_relatives(upstream=upstream) is None:
        return dependants

    for t in task.get_direct_relatives(upstream=upstream):
        dependants.update(
            recur_airflow_dependants(t, dependants=dependants, upstream=upstream)
        )

    return dependants


def get_all_airflow_dependants(
    task_or_task_list: Union[Operator, List[Operator]],
    upstream: bool = False,
) -> List[Operator]:
    """
    Returns a list of directly and indirectly dependant tasks.
    """
    try:
        task_list = list(task_or_task_list)  # type: ignore
    except TypeError:
        task_list = [task_or_task_list]  # type: ignore

    dependant_tasks = set()

    for task in task_list:
        dependant_tasks.update(
            recur_airflow_dependants(task, dependants=set(), upstream=upstream)
        )

    return list(dependant_tasks)
