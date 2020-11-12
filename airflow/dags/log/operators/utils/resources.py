import json

from typing import Dict, List, Union, Optional


class ResourcesList:
    def __init__(self, resources):
        """Creates dict of Resources with country lists and default-flags"""
        if isinstance(resources, str):
            resources = json.loads(resources)

        # keys to be passed to Resources(..)
        keys_spec = ['request_memory', 'request_cpu', 'limit_memory', 'limit_cpu', 'limit_gpu']

        # if only one resource is provided, add to list and set to default
        if keys_spec[0] in resources.keys() or keys_spec[1] in resources.keys():
            resources_list = {
                'default': {
                    'resources': {key: value for key, value in resources.items() if key in keys_spec},
                    'countries': resources.get('countries', []),
                    'is_default': True
                }
            }
        else:
            resources_list = {
                size: {
                    'resources': {key: value for key, value in values.items() if key in keys_spec},
                    'countries': values.get('countries', []),
                    # if only one resource is in the list, set to default
                    'is_default': values.get('is_default', False) if len(resources) > 1 else True,
                } for size, values in resources.items()
            }

        self.resources_list = resources_list

        self.resources_default = None
        for size, values in self.resources_list.items():
            if values['is_default']:
                self.resources_default = values['resources']

    def get_default(self):
        """Returns default resource if defined (or if there is only one resource available)"""
        if self.resources_default is None:
            raise KeyError('No default resources defined.')
        return self.resources_default

    def get(self, country_code=None):
        """Returns country-specific resource if defined or default resource if undefined."""
        for size, values in self.resources_list.items():
            if country_code in values['countries']:
                return values['resources']
        return self.get_default()


"""
Buliding blocks:
- Resources: top-level class associated with the complete required JSON format for resource specification
- ResourceGroup: class associated with a resource group e.g. contains small, medium, large etc. resources
- Resource: class associated with a single resources specification

JSON structure of building blocks: <Resources>, <ResourceGroup>, <Resource>

Resources:

    Valid examples:

        1. Full example:
            {
                "common": <ResourceGroup>,
                "task-specific": {
                    "process-country-data": <ResourceGroup>,
                    "train-country-model": <ResourceGroup>,
                    ...
                }
            }

        2. Simplified:
            <ResourceGroup>

        2. Not specified:
            <ResourceGroup>


ResourceGroup:

    {
        "default": <Resource>,
        "small": <Resource>,
        "large": <Resource>
    }

    Note: "default" key is not required

Resource:

    {
        "request_cpu": "200m",
        "limit_cpu": "500m",
        "request_memory": "300Mi",
        "limit_memory": "800Mi"
        "countries": ["sg", "pk"]
    }

"""


class ResourcesException(Exception):
    pass


class Resource:
    """
    Class associated with one resource specification
    """
    request_memory: str
    request_cpu: str
    limit_memory: str
    limit_cpu: str
    countries: List[str]

    def __init__(self, resource_spec: Dict):
        self.countries = resource_spec.get('countries')
        try:
            self.request_memory = resource_spec['request_memory']
            self.request_cpu = resource_spec['request_cpu']
            self.limit_memory = resource_spec['limit_memory']
            self.limit_cpu = resource_spec['limit_cpu']
        except KeyError as e:
            raise ResourcesException(e)

    def get(self):
        return {
            'request_memory': self.request_memory,
            'request_cpu': self.request_cpu,
            'limit_memory': self.limit_memory,
            'limit_cpu': self.limit_cpu
        }


DEFAULT_RESOURCE_SPEC = {
    "request_cpu": "200m",
    "limit_cpu": "500m",
    "request_memory": "300Mi",
    "limit_memory": "800Mi",
}


class ResourceGroup:
    """
    Class associated with a resource group e.g. contains small, medium, large etc. resources
    """
    default: Resource
    resources: Dict[str, Resource]

    def __init__(self, resources_group_spec: Dict):
        self.default = Resource(resources_group_spec['default']) if 'default' in resources_group_spec else None
        self.resources = {
            name: Resource(resources_group_spec)
            for name, resources_group_spec in resources_group_spec.items() if name != 'default'
        }

    def get(self, country_code: str):
        """
        Finds the first resource that contains the country code
        """
        resource = next((resource.get() for resource in self.resources.values() if country_code in resource.countries), None)

        if resource is not None:
            return resource

        return self.get_default()

    def get_default(self):
        return self.default.get() if self.default else None


class Resources:
    """
    Class associated with the complete resource object for a DAG
    """
    common: ResourceGroup
    task_specific: Dict[str, ResourceGroup]

    def __init__(self, resources_spec: Optional[Union[Dict, str]],
                 default=None):

        if default is None:
            default = DEFAULT_RESOURCE_SPEC
        if resources_spec is None:
            resources_spec = {"common": {"default": default}}
        elif isinstance(resources_spec, str):
            resources_spec = json.loads(resources_spec)

        if 'common' not in resources_spec:  # When we don't need task-specific resources
            resources_spec["common"] = {"default": default}

        if "default" not in resources_spec["common"]:  # In case of missing default specification we use the one from code
            resources_spec["common"]["default"] = default

        self.common = ResourceGroup(resources_spec['common'])

        task_specific_spec = resources_spec.get('task-specific', {})  # Initialize task-specific resources

        self.task_specific = {
            task: ResourceGroup(resource_group_spec)
            for task, resource_group_spec in task_specific_spec.items()
        }

    def get(self, country_code: str = None, task_id: str = None) -> Dict[str, str]:
        """
        Extracts the resource in following precedence:
        1. task-specific resources
        2. common resources
        3. defaults
        """
        resource = None
        if task_id in self.task_specific:
            resource = self.task_specific[task_id].get(country_code)

        if resource is None:
            resource = self.common.get(country_code)

        if resource is None:
            resource = self.get_default(country_code, task_id)

        return resource

    def get_default(self, country_code: str = None, task_id: str = None):
        """
        Get the default resource in following precendence:
        1. task-specific resources
        2. common resources
        Otherwise raises an error
        """
        resource = None

        if task_id and task_id in self.task_specific:
            resource = self.task_specific[task_id].get(country_code)

        if resource is None:
            resource = self.common.get_default()

        return resource
