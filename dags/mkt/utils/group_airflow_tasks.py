import itertools
from airflow.operators.dummy_operator import DummyOperator

# Tableau 10 Light
# from https://github.com/jiffyclub/palettable/blob/768d5ef8d14437d4bffa27afb96e4990c919a25d/palettable/tableau/tableau.py#L50-L60  # noqa
COLOR_PALETTE = [
    "#AEC7E8",
    "#FFBB78",
    "#98DF8A",
    "#FF9896",
    "#C5B0D5",
    "#C49C94",
    "#F7B6D2",
    "#C7C7C7",
    "#DBDB8D",
    "#9EDAE5",
]
COLOR_GENERATOR = itertools.cycle(COLOR_PALETTE)


def group_airflow_tasks(task_or_task_list, group_name, custom_ui_color=None):
    """
    Helper function to group a list of Airflow tasks in a DAG.

    Returns a tuple of two DummyOperators start_task and finish_task, which can be used to define
    high-level dependencies in the DAG. All root-like task of the task list while be assigned
    as downstream dependencies of start_task while all leave-like tasks will be assigned as
    upstream dependencies of finish_task.

    task_list: list or single task instance
    group_name: str

    returns: (start_task, finish_task)
    """
    try:
        task_list = list(task_or_task_list)  # type: ignore
    except TypeError:
        task_list = [task_or_task_list]  # type: ignore

    if not custom_ui_color:
        _ui_color = next(COLOR_GENERATOR)
    else:
        _ui_color = custom_ui_color

    start_task = DummyOperator(task_id=f"start_{group_name}")
    start_task.ui_color = _ui_color
    finish_task = DummyOperator(task_id=f"finish_{group_name}")
    finish_task.ui_color = _ui_color

    for task in task_list:
        # tasks without upstream dependencies are root-like
        if not task.get_direct_relatives(upstream=True):
            start_task >> task

        # tasks without upstream dependencies are leave-like
        if not task.get_direct_relatives(upstream=False):
            task >> finish_task

    return (start_task, finish_task)
