import json
import unittest

from operators.utils.resources import Resources, DEFAULT_RESOURCE_SPEC

# Atomic Resource class fixtures
resource_small = {
    'request_cpu': '200m',
    'limit_cpu': '400m',
    'request_memory': '200Mi',
    'limit_memory': '400Mi'
}

resource_large = {
    'request_cpu': '400m',
    'limit_cpu': '800m',
    'request_memory': '400Mi',
    'limit_memory': '800Mi'
}

resource_xl = {
    'request_cpu': '800m',
    'limit_cpu': '1600m',
    'request_memory': '800Mi',
    'limit_memory': '1600Mi'
}

# Resources class fixtures
fixture_resources_default = {
    'common': {
        'default': resource_small
    }
}

fixture_resources_common = {
    'common': {
        'default': resource_small,
        'large': {
            'countries': ['sg', 'pk'],
            **resource_large
        }
    }
}

fixture_resources_task_specific = {
    'common': {
        'default': resource_small,
        'large': {
            'countries': ['sg', 'pk'],
            **resource_large
        }
    },
    'task-specific': {
        'train-model': {
            'default': resource_large,
            'xl': {
                'countries': ['th', 'eg'],
                **resource_xl
            }
        }
    }
}


class ResourcesTest(unittest.TestCase):

    def test_resources_none(self):
        resources = Resources(None)

        self.assertEqual(resources.get("xx", "validate-country-model"), DEFAULT_RESOURCE_SPEC)

    def test_resources_default_missing(self):
        resources_spec = {
            "common": {
                "large": {
                    **resource_large,
                    'countries': ['pk', 'sg']
                }
            }
        }
        resources = Resources(json.dumps(resources_spec))

        self.assertEqual(resources.get("xx", "validate-country-model"), DEFAULT_RESOURCE_SPEC)

    def test_resources_default(self):
        resources = Resources(json.dumps(fixture_resources_default))

        self.assertEqual(resources.get_default(), resource_small)
        self.assertEqual(resources.get('sg'), resource_small)
        self.assertEqual(resources.get('sg', 'train-model'), resource_small)

    def test_resources_common(self):
        resources = Resources(json.dumps(fixture_resources_common))

        self.assertEqual(resources.get_default(), resource_small)
        self.assertEqual(resources.get('sg'), resource_large)
        self.assertEqual(resources.get('sg', 'train-model'), resource_large)  # the task is not found, so it defaults to default
        self.assertEqual(resources.get(None, 'train-model'), resource_small)
        self.assertEqual(resources.get(None, None), resource_small)

    def test_resources_task_specific(self):
        resources = Resources(json.dumps(fixture_resources_task_specific))

        self.assertEqual(resources.get_default(), resource_small)
        self.assertEqual(resources.get('sg'), resource_large)

        self.assertEqual(resources.get('th'), resource_small)  # th is only specified in task-specific resources, so common default is used
        self.assertEqual(resources.get('th', 'train-model'), resource_xl)

        # If country is not in task specific resources, default to task-specific default
        self.assertEqual(resources.get('ph', 'train-model'), resource_large)

        # If default is not present in task-specific, we return the resource from datahub.common
        fixture_resources_task_specific_no_default = fixture_resources_task_specific.copy()
        del fixture_resources_task_specific_no_default['task-specific']['train-model']['default']
        resources_task_specific_no_default = Resources(json.dumps(fixture_resources_task_specific_no_default))

        self.assertEqual(resources_task_specific_no_default.get('ph', 'train-model'), resource_small)
        self.assertEqual(resources_task_specific_no_default.get('sg', 'train-model'), resource_large)
