import unittest

from operators.utils.resources import ResourcesList


class ResourceListTest(unittest.TestCase):

    def test_single_resources(self):
        resources = {
            'request_cpu': '200m',
            'limit_cpu': '400m',
            'request_memory': '200Mi',
            'limit_memory': '400Mi'
        }
        resources_list = ResourcesList(resources)
        self.assertEqual(resources_list.get_default(), resources)
        self.assertEqual(resources_list.get(), resources)
        self.assertEqual(resources_list.get('xy'), resources)

        resources_list = ResourcesList({'some_name': resources})
        self.assertEqual(resources_list.get_default(), resources)
        self.assertEqual(resources_list.get(), resources)
        self.assertEqual(resources_list.get('xy'), resources)

    def test_multiple_resources(self):
        resources_default = {
            'request_cpu': '1m',
            'limit_cpu': '2m',
            'request_memory': '3Mi',
            'limit_memory': '4Mi'
        }
        resources_large = {
            'request_cpu': '5m',
            'limit_cpu': '6m',
            'request_memory': '7Mi',
            'limit_memory': '8Mi',
        }
        resources = {
            'small': {
                'is_default': True,
                **resources_default,
            },
            'large': {
                'countries': ['xy'],
                **resources_large
            }
        }
        resources_list = ResourcesList(resources)

        self.assertEqual(resources_list.get_default(), resources_default)

        # specifying no country_code should fetch default resources
        self.assertEqual(resources_list.get_default(), resources_list.get())

        # xy should be large, ab should be default
        self.assertEqual(resources_list.get('xy'), resources_large)
        self.assertEqual(resources_list.get('ab'), resources_default)

    def test_muliple_resources_default_undefined(self):
        resources_default = {
            'request_cpu': '1m',
            'limit_cpu': '2m',
            'request_memory': '3Mi',
            'limit_memory': '4Mi'
        }
        resources_large = {
            'request_cpu': '5m',
            'limit_cpu': '6m',
            'request_memory': '7Mi',
            'limit_memory': '8Mi',
        }
        resources = {
            'small': {
                **resources_default,
            },
            'large': {
                'countries': ['xy'],
                **resources_large
            }
        }
        resources_list = ResourcesList(resources)

        self.assertEqual(resources_list.get('xy'), resources_large)

        with self.assertRaises(KeyError):
            resources_list.get('ab')

        with self.assertRaises(KeyError):
            resources_list.get_default()
