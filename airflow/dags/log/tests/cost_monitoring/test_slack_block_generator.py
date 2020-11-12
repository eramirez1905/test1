import unittest
from unittest.mock import Mock

from freezegun import freeze_time

from cost_monitoring.slack_block_generator import context_generator, block_generator


class SlackBlockGeneratorTest(unittest.TestCase):

    def test_context_generator(self):
        response = {
            'date': '2020-01-15',
            'user_id': 'devin@deliveryhero.com',
            'total_cost': '140'
        }

        expected = {
            'type': 'context',
            'elements': [
                {
                    'type': 'mrkdwn',
                    'text': 'The user <@DUGJD> has spent `140 Euros` in Big Query on 2020-01-15'
                }
            ]
        }

        mock = Mock()  # Mock Slack Helper
        mock.get_user_id.return_value = {
            'success': True,
            'mention_id': '<@DUGJD>',
            'user': 'DUGJD',

        }
        self.assertEqual(expected, context_generator(response=response, slack=mock))

    def test_context_generator_of_user_not_found(self):
        response = {
            'date': '2020-01-15',
            'user_id': 'devin@deliveryhero.com',
            'total_cost': '140'
        }

        expected = {
            'type': 'context',
            'elements': [
                {
                    'type': 'mrkdwn',
                    'text': 'The user devin has spent `140 Euros` in Big Query on 2020-01-15'
                }
            ]
        }

        mock = Mock()  # Mock Slack Helper
        mock.get_user_id.return_value = {
            'success': False,
            'mention_id': 'devin',
            'user': 'devin',

        }
        self.assertEqual(expected, context_generator(response=response, slack=mock))

    @freeze_time("2020-01-15")
    def test_block_generator(self):
        responses = [
            {
                'date': '2020-01-15',
                'user_id': 'devin@deliveryhero.com',
                'total_cost': '140'
            },
            {
                'date': '2020-01-14',
                'user_id': 'aless@deliveryhero.com',
                'total_cost': '99'
            }
        ]

        expected = [
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': 'Cost Monitoring at 2020-01-15 00:00:00'
                }

            },
            {
                'type': 'divider'
            },
            {
                'type': 'context',
                'elements': [
                    {
                        'type': 'mrkdwn',
                        'text': 'The user <@DUGJD> has spent `140 Euros` in Big Query on 2020-01-15'
                    }
                ]
            },
            {
                'type': 'divider'
            },
            {
                'type': 'context',
                'elements': [
                    {
                        'type': 'mrkdwn',
                        'text': 'The user <@LESA> has spent `99 Euros` in Big Query on 2020-01-14'
                    }
                ]
            },
            {
                'type': 'divider'
            }
        ]

        mock = Mock()
        mock.get_user_id.side_effect = [
            {
                'success': True,
                'mention_id': '<@DUGJD>',
                'user': 'DUGJD',
            },
            {
                'success': True,
                'mention_id': '<@LESA>',
                'user': 'LESA',
            }]
        self.assertEqual(expected, block_generator(responses=responses, slack=mock))
