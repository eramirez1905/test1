import datetime


def context_generator(response, slack):
    date = response.get('date')
    user_id = response.get('user_id')
    # Only find the ID's of users with email deliveryhero.com
    if '@deliveryhero.com' in user_id:
        user_id = __get_user_id(slack=slack, user_id=user_id)

    total_cost = response.get('total_cost')

    context_elements = {
        'type': 'mrkdwn',
        'text': f'The user {user_id} has spent `{total_cost} Euros` in Big Query on {date}'
    }

    return {
        'type': 'context',
        'elements': [context_elements]
    }


def __get_user_id(slack, user_id):
    response = slack.get_user_id(user_email=user_id)
    user_id = response.get('user')
    if response.get('success'):
        slack.invite_user_to_channel(user_id=user_id)
        user_id = response.get('mention_id')
    return user_id


def block_generator(responses, slack):
    first_block = {
        'type': 'section',
        'text': {
            'type': 'mrkdwn',
            'text': f'Cost Monitoring at {datetime.datetime.now()}'
        }
    }
    divider = {
        'type': 'divider'
    }
    blocks = [first_block, divider]
    for response in responses:
        contexts = context_generator(response, slack)
        blocks.append(contexts)
        blocks.append(divider)
    return blocks
