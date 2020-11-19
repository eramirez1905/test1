import unittest
from unittest.mock import Mock

from cost_monitoring.slack_helper import SlackHelper


class SlackHelperTest(unittest.TestCase):

    def test_finding_foodora_user(self):
        mock = Mock()
        mock.api_call.side_effect = [
            {
                'ok': False,
                'error': 'users_not_found'  # Delivery Hero API call
            },
            {
                'ok': True,
                'user': {
                    'id': 'odracci'  # Foodora API call
                }
            }
        ]

        slack_helper = SlackHelper(slack=mock)
        user_id = slack_helper.get_user_id('riccardo@deliveryhero.com')
        expected_response = {
            'success': True,
            'mention_id': '<@odracci>',
            'user': 'odracci',
        }

        self.assertEqual(expected_response, user_id)

    def test_user_not_found(self):
        mock = Mock()
        mock.api_call.return_value = {
            'ok': False,
            'error': 'users_not_found',
        }

        slack_helper = SlackHelper(slack=mock)
        user_id = slack_helper.get_user_id('aless@deliveryhero.com')
        expected_response = {
            'success': False,
            'mention_id': None,
            'user': 'aless',
        }

        self.assertEqual(expected_response, user_id)

    def test_user_found(self):
        mock = Mock()
        mock.api_call.return_value = {
            'ok': True,
            'user': {
                'id': 'DUGJD'
            }
        }

        slack_helper = SlackHelper(slack=mock)
        user_id = slack_helper.get_user_id('aless@deliveryhero.com')
        expected_response = {
            'success': True,
            'mention_id': '<@DUGJD>',
            'user': 'DUGJD',
        }

        self.assertEqual(expected_response, user_id)
