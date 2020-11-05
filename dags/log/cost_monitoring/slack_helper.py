from configuration import config
import json

from airflow.utils.log.logging_mixin import LoggingMixin


class SlackHelper(LoggingMixin):

    def __init__(self, slack):
        super().__init__()
        self.slack = slack

    def get_user_id(self, user_email):
        user = self.__get_user(user_email=user_email)
        if not user.get('success'):
            user = self.__get_user(user_email=user_email.replace('deliveryhero.com', 'foodora.com'))

        return user

    def __get_user(self, user_email):
        try:
            response = self.__api_call(user_email=user_email)
            user = response.get('user').get('id')
            return self.__generate_response(True, '<@' + user + '>', user)
        except Exception:
            return self.__generate_response(False, None, user_email.split('@')[0])

    def __api_call(self, user_email):
        response = self.slack.api_call(
            method='users.lookupByEmail',
            email=user_email,
        )
        self.log.info(f'Get user email {user_email} response: {response}')

        if not response.get('ok'):
            raise Exception

        return response

    @staticmethod
    def __generate_response(success, mention_id, user):
        return {
            'success': success,
            'mention_id': mention_id,
            'user': user
        }

    def invite_user_to_channel(self, user_id):
        response = self.slack.api_call(
            method='channels.invite',
            channel=config['slack'].get("channel_cost_monitoring").get("id"),
            user=user_id,
        )
        self.log.info(f'Invite user `{user_id}` to channel response {response}')

    def post_message(self, channel, username, blocks):
        response = self.slack.api_call(
            method='chat.postMessage',
            channel=channel,
            username=username,
            blocks=json.dumps(blocks),
            link_names=True,
            text="Big Query Cost Monitoring",
        )
        self.log.info(f'Post Message response {response}')
