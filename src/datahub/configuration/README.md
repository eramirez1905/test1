# Configuration

## Table of Contents

 - [How to use](#how-to-use)
 - [How it works](#how-it-works)
 - [Convention](#convention)
 - [Examples](#examples)

## How to use

```python
from configuration import config


CONFIG_ONE = config['best_project']['configuration_option']
```

## How it works

The config object created in `dags/{BU}/configuration/__init__.py` is an `OrderedDict` instance,
created by merging all of the configuration files available in `configuration` directory.

Matching keys across multiple files will be overwritten unless the value assigned to the key
is a list; In that case, it will be extended with a list from the other file. For more information,
read: [hiyapyco](https://github.com/zerwes/hiyapyco)

The loading order of the configuration files is the following:

1. Common DataHub default files, e.g. `src/datahub/common/yaml/spark.yaml`
2. Common DataHub environment specific files, e.g. `src/datahub/common/yaml/spark_production.yaml`
3. Common default configurations, i.e. `dags/{BU}/configuration/yaml/config.yaml`
4. Common, environment specific configurations, e.g. `dags/{BU}/configuration/yaml/config_production.yaml`
5. Project specific default file, e.g.
`dags/{BU}/configuration/yaml/best_project/best_project.yaml`
6. Project-specific environment specific file, e.g.
`dags/{BU}/configuration/yaml/best_project/best_project_production.yaml`
7. `configuration` Airflow Variable if available in env.

## Convention

 - Each configuration file should be in the `YAML` format.
 - Each project-specific configuration files should be stored in a separate directory, named with
the project itself. For example, the `curated_data` project configuration should be within a
folder named `curated_data`.
 - The file name for project-specific configurations should be named as the project itself.
For example, the `curated_data` configuration should be named `curated_data.yaml`. In case
environments have different configuration between themselves, please use the corresponding
suffix for the configuration file: `_dev`, `_staging`, `_production`, `_test`.
 - Keys between different files in each BU configuration directory **must** be disjoint.
You shouldn't override common BU configuration files with project-specific ones.
- You **shouldn't** put production configuration options in default config file and override it with
`_dev` or `_staging`. Instead, put specific options at specific levels or use `test/dev` options in
the default file and overwrite it with `_production`.

## Examples:

#### Dir tree

```
configuration
└──yaml
   ├── best_project
   │   ├── best_project.yaml
   │   ├── best_project_production.yaml
   │   └── ...
   ├── __init__.py
   ├── config.yaml
   ├── config_dev.yaml
   ├── config_production.yaml
   ├── config_staging.yaml
   └── config_test.yaml
```

#### best_project.yaml contents

```yaml
best_project:
  table: 'best_table'
  s3_conn: 's3_bucket'
  key1:
    - 1
    - 2
```

#### best_project_production.yaml contents

```yaml
best_project:
  table: 'best_prod_table'
  key1:
    - 5
    - 8
```

#### config.yaml contents

```yaml
bigquery:
  project: 'test_project'
```

#### config_production.yaml contents

```yaml
bigquery:
  project: 'prod_project'
```

#### final config contents in prod env

```python
>>> from configuration import config
>>> print(config)
{
  'best_project': {
    'key1': [1, 2, 5, 8],
    's3_conn': 's3_bucket',
    'table': 'best_prod_table'
  },
  'bigquery': {
    'project': 'prod_project'
  },
  ... # common DataHub config
}
```
