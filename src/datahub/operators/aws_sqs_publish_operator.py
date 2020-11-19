# TODO: this is a copy of
# https://github.com/apache/airflow/blob/1.10.4rc1/airflow/contrib/operators/aws_sqs_publish_operator.py
# and can be removed once we are on 1.10.4

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datahub.hooks.aws_sqs_hook import SQSHook


class SQSPublishOperator(BaseOperator):
    """
    Publish message to a SQS queue.
    :param sqs_queue: The SQS queue url (templated)
    :type sqs_queue: str
    :param message_content: The message content (templated)
    :type message_content: str
    :param message_attributes: additional attributes for the message (default: None)
        For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
    :type message_attributes: dict
    :param delay_seconds: message delay (templated) (default: 1 second)
    :type delay_seconds: int
    :param aws_conn_id: AWS connection id (default: aws_default)
    :type aws_conn_id: str
    """

    template_fields = ('sqs_queue', 'message_content', 'delay_seconds')
    ui_color = '#6ad3fa'

    @apply_defaults
    def __init__(self,
                 sqs_queue,
                 message_content,
                 message_attributes=None,
                 delay_seconds=0,
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.message_content = message_content
        self.delay_seconds = delay_seconds
        self.message_attributes = message_attributes or {}

    def execute(self, context):
        """
        Publish the message to SQS queue
        :param context: the context object
        :type context: dict
        :return: dict with information about the message sent
            For details of the returned dict see :py:meth:`botocore.client.SQS.send_message`
        :rtype: dict
        """

        hook = SQSHook(aws_conn_id=self.aws_conn_id)

        result = hook.send_message(queue_url=self.sqs_queue,
                                   message_body=self.message_content,
                                   delay_seconds=self.delay_seconds,
                                   message_attributes=self.message_attributes)

        self.log.info('result is send_message is %s', result)

        return result
