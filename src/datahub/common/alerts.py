from datetime import datetime, timedelta

import pytz
from airflow import LoggingMixin, AirflowException
from airflow.contrib.operators.opsgenie_alert_operator import OpsgenieAlertOperator
from airflow.models import TaskInstance, DAG
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.utils.state import State as AirflowState

from configuration import config

log = LoggingMixin().log


def setup_callback(is_opsgenie_is_enabled=False, opsgenie_priority='P5') -> callable:
    def send_slack_notification(context):
        log.info('Sending slack notification')
        __create_slack_operator(context).execute()

    def send_opsgenie_notification(context):
        for param in ['responders', 'visibleTo']:
            if 'opsgenie' not in config and not config['opsgenie'][param]:
                raise AirflowException('No configuration created for opsgenie: missing %s parameter.', param)
        log.info('Sending opsgenie notification')
        __create_opsgenie_operator(context, opsgenie_priority).execute(context=context)

    def run(context):
        exceptions = []
        try:
            send_slack_notification(context)
        except Exception as err:
            log.exception('Error while running Slack alert callback')
            exceptions.append(err)

        if is_opsgenie_is_enabled:
            try:
                send_opsgenie_notification(context)
            except Exception as err:
                log.exception('Error while running OpsGenie alert callback')
                exceptions.append(err)

        if len(exceptions) > 0:
            raise AirflowException('Not able to send the alert notifications. Please check the logs for details.')

    return run


def alert_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    :param blocking_tis:
    :type blocking_tis: list of airflow.models.TaskInstance
    :param blocking_task_list:
    :type blocking_task_list: list of str
    :param task_list:
    :type task_list: list of str
    :type slas: list of airflow.models.SlaMiss
    :param slas:
    :type dag: airflow.models.DAG
    """

    message = 'SLA was missed on DAG %(dag)s by task id:' \
              '\n' \
              '\n %(blocking_tis)s' \
              '\n with task list' \
              '\n %(task_list)s which are blocking' \
              '\n %(blocking_task_list)s' % locals()

    log.error(message)

    alert = SlackAPIPostOperator(
        task_id='slack_sla_miss',
        channel=__get_channel(dag.owner, 'channel_errors'),
        slack_conn_id='slack_default',
        owner=dag.owner,
        text=message,
        blocks=list(),
    )
    alert.execute()


def __get_channel(owner, channel_name):
    return config['teams'].get(owner, {}).get(channel_name, config['slack'].get(channel_name))


def __create_state_information(state, dag):
    state_information = {
        AirflowState.UP_FOR_RETRY: {
            'task_id': 'slack_retry',
            'state_icon': ':white_circle:',
            'channel': __get_channel(dag.owner, 'channel_errors'),
            'color': '#FFFFFF',
        },
        AirflowState.SUCCESS: {
            'task_id': 'slack_success',
            'state_icon': ':greenball:',
            'channel': __get_channel(dag.owner, 'channel'),
            'color': 'good',
        },
        AirflowState.FAILED: {
            'task_id': 'slack_failed',
            'state_icon': ':red_circle:',
            'channel': __get_channel(dag.owner, 'channel_errors'),
            'color': 'danger',
        },
    }
    return state_information.get(state, state_information.get(AirflowState.FAILED))


def __create_slack_operator(context, pretext=''):
    link, datetime_log, dag, task_instance = __extract_context_details(context)
    state = task_instance.state

    state_information = __create_state_information(state=state, dag=dag)

    message = f'"{dag.dag_id}" {state_information.get("state_icon")} {state} at {datetime_log}'
    fallback_message = f'{state_information.get("state_icon")} {message}: See logs at <{link}|logs>'

    alert = SlackAPIPostOperator(
        task_id=state_information.get('task_id'),
        channel=state_information.get('channel'),
        slack_conn_id='slack_default',
        owner=dag.owner,
        attachments=__create_attachments(
            context=context,
            link=link,
            fallback_message=fallback_message,
            task_instance=task_instance,
            owner=dag.owner,
            color=state_information.get('color'),
            pretext=pretext
        ),
        text=message,
        blocks=list(),
    )

    return alert


def __create_opsgenie_operator(context, opsgenie_priority):
    link, datetime_log, dag, task_instance = __extract_context_details(context)
    state = task_instance.state

    if state == AirflowState.FAILED:
        task_id = 'opsgenie_failed'
    else:
        raise Exception(f'state "{state}" not defined for opsgenie callback.')

    message = f'"{dag.dag_id}" {state} at {datetime_log}'
    details = __create_opsgenie_details(context, link, task_instance)

    alert = OpsgenieAlertOperator(
        task_id=task_id,
        message=message,
        description='Airflow Opsgenie Alert',
        responders=config['opsgenie']['responders'],
        visibleTo=config['opsgenie']['visibleTo'],
        details=details,
        priority=opsgenie_priority,
    )

    return alert


def __create_opsgenie_details(context, link: str, task_instance: TaskInstance):
    start_date, end_date, duration = __extract_task_instance_details(task_instance)

    details = {
        "DAG": context['dag'].dag_id,
        "Task ID": task_instance.task_id if task_instance is not None else 'Unknown',
        "Run ID": context['run_id'],
        "State": task_instance.state,
        "Execution": context['execution_date'].strftime('%Y/%m/%d %H:%M:%S'),
        "Execution date (Berlin TZ)": context['execution_date'].astimezone(pytz.timezone('Europe/Berlin')).strftime(
            '%Y/%m/%d %H:%M:%S'),
        "Start date (Berlin TZ)": start_date,
        "end Date (Berlin TZ)": end_date,
        "Duration": duration,
        "See Logs": link,
    }

    return details


def __extract_task_instance_details(task_instance: TaskInstance):
    start_date = task_instance.start_date.astimezone(pytz.timezone('Europe/Berlin')).strftime(
        '%Y/%m/%d %H:%M:%S') if task_instance.start_date else ''
    end_date = task_instance.end_date.astimezone(pytz.timezone('Europe/Berlin')).strftime(
        '%Y/%m/%d %H:%M:%S') if task_instance.end_date else ''

    duration = 'N/A'
    if task_instance.duration is not None:
        duration = str(timedelta(seconds=task_instance.duration))

    return start_date, end_date, duration


def __extract_context_details(context):
    link: str = context.get('task_instance').log_url if context.get('task_instance') else "N/A"
    datetime_log: str = datetime.now(pytz.timezone('Europe/Berlin')).strftime('%Y/%m/%d %H:%M:%S')
    dag: DAG = context['dag']
    task_instance: TaskInstance = context['task_instance']

    return link, datetime_log, dag, task_instance


def __create_attachments(
        context,
        link: str,
        fallback_message: str,
        task_instance: TaskInstance,
        owner: str,
        color: str,
        pretext=''):
    if not config['slack']['has_mentions']:
        owner = None

    owners = config['teams'].get(owner, {}).get('members', [])
    owners_field = []
    if owners and task_instance.state != AirflowState.SUCCESS:
        owners_field = [
            {
                "title": "Owners",
                "value": f"<@{'>, <@'.join(owners)}>",
                "short": True
            }
        ]

    start_date, end_date, duration = __extract_task_instance_details(task_instance)

    attachments = [
        {
            "fallback": fallback_message,
            "color": color,
            "pretext": pretext,
            "fields": [
                {
                    "title": "DAG",
                    "value": context['dag'].dag_id,
                    "short": True
                },
                {
                    "title": "Task",
                    "value": task_instance.task_id if task_instance is not None else 'Unknown',
                    "short": True
                },
                {
                    "title": "Run ID",
                    "value": context['run_id'],
                    "short": True
                },
                {
                    "title": "State",
                    "value": f'{task_instance.state}',
                    "short": True
                },
                {
                    "title": "Execution date",
                    "value": context['execution_date'].strftime('%Y/%m/%d %H:%M:%S'),
                    "short": True
                },
                {
                    "title": "Execution date (Berlin TZ)",
                    "value": context['execution_date'].astimezone(pytz.timezone('Europe/Berlin')).strftime(
                        '%Y/%m/%d %H:%M:%S'),
                    "short": True
                },
                {
                    "title": "Start date (Berlin TZ)",
                    "value": start_date,
                    "short": True
                },
                {
                    "title": "end Date (Berlin TZ)",
                    "value": end_date,
                    "short": True
                },
                {
                    "title": "Duration",
                    "value": duration,
                    "short": True
                },
                *owners_field
            ],
            "actions": [
                {
                    "type": "button",
                    "text": "See Logs",
                    "url": link,
                }
            ]
        }
    ]

    return attachments
