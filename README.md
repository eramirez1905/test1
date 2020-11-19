# DH DataHub Airflow

This repository contains the DAGs running in Airflow and the setup or the Docker image running the jobs. More information at [https://airflow.apache.org](https://airflow.apache.org).

Great list of resources [https://github.com/jghoman/awesome-apache-airflow](https://github.com/jghoman/awesome-apache-airflow)

# Add a new Business Unit

Run the following command and follow the instructions. Replace `BUSINESS_UNIT` with the name of the business unit

```bash
$ docker-compose run --rm webserver python bin/create_business_unit.py fulfillment-dwh-staging $BUSINESS_UNIT
$ docker-compose run --rm webserver python bin/create_business_unit.py fulfillment-dwh-production $BUSINESS_UNIT
```

Once the new business unit folder has been created, it is recommended to search for all the TODOs withing such a folder and apply the corresponding changes. 

# Setup local development environment

- Please set an environment variable according to your team (E.g. log or pandata) in your `~/.bashrc` or `~/.zshrc` file.

```bash
$ export AIRFLOW_BUSINESS_UNIT="log"
```

- Build the container

```bash
$ docker-compose build
```

- Optional: If you had a local airflow environment before and you are not keen to keep the data in your local airflow MySQL db you can also delete it via `rm -r data` to have a fresh start. But you will lose all dag run data!

- If it is the first time, run the following command as it creates Airflow tables and the admin user:

```bash
$ docker-compose run --rm webserver initdb
$ docker-compose run --rm webserver create_user -u airflow -l airflow -f jon -e airflow@apache.org -r Admin -p airflow
$ AIRFLOW__WEBSERVER__UPDATE_FAB_PERMS=True docker-compose run --rm webserver sync_perm
```

- Run Airflow database migrations:

```bash
$ docker-compose run --rm webserver upgradedb
```

- Start the Web Server

```bash
$ docker-compose up webserver
```

- Add a Business unit:

Add a new database in `docker/mysql/docker-entrypoint-initdb.d/50-create-databases.sql`
If the folder `data/mysql` exists (not the first time setup), create the database:

```shell script
$ docker-compose exec mysql bash -c "mysql -u root -pairflow airflow_logistics < /docker-entrypoint-initdb.d/50-create-databases.sql"
```


## In case you had an already existing local environment

- Please set an environment variable according to your team (E.g. log or pandata) in your `~/.bashrc` or `~/.zshrc` file.

```bash
$ export AIRFLOW_BUSINESS_UNIT="log"
```

- Optional: take a backup

```bash
$ docker-compose mysql -d up
$ docker-compose exec mysql bash -c "mysqldump -u root -pairflow airflow_logistics >/docker-entrypoint-initdb.d/airflow.sql"
$ mv docker/mysql/docker-entrypoint-initdb.d/airflow.sql .
```

- Stop the container mysql and delete it

```bash
$ docker-compose stop mysql
$ docker-compose rm mysql
```

- Delete delete data folder (it contains the mysql DB, take a backup if you need it)

```bash
$ rm -rf ./data
```

- Optional: Restore the backup

```bash
$ docker-compose up mysql
# When MySQL is up and running
$ mv airflow.sql docker/mysql/docker-entrypoint-initdb.d/airflow.sql
$ docker-compose exec mysql bash -c "mysql -u root -pairflow airflow_log < /docker-entrypoint-initdb.d/airflow.sql"
$ rm docker/mysql/docker-entrypoint-initdb.d/airflow.sql
```

- Finally, run

```bash
$ docker-compose up -d mysql
$ docker-compose up webserver
```


### Setup BigQuery credentials

- Install [Google SDK](https://cloud.google.com/sdk/)
- Enable application default credentials:

```bash
$ gcloud auth application-default login
$ gcloud config set project fulfillment-dwh-staging
```
- Open the [web UI](#Web-UI), go to the list of connections (Admin -> Connections),
edit `bigquery_default` and fill the field `Project Id` with `fulfillment-dwh-staging`

Gotcha: Remove the `quota_project_id` field from `~/.config/gcloud/application_default_credentials.json`
because Airflow throws an error if it exists.


# Running locally

### Run on Docker (recommended)

##### Run the mysql in background:

```bash
$ docker-compose up -d mysql
```

##### Run the web server:

```bash
$ docker-compose up webserver
```

##### Run the scheduler (optional)

```bash
$ docker-compose -f docker-compose-scheduler.yml up
```

### Run on Kubernetes (beta)

Please follow the instructions in [logistics-dwh/README.md](https://github.com/foodora/logistics-dwh/blob/master/README.md)

#### Run a task instance:

To run a single task instance without the scheduler (and without clearing or running any dependencies in the UI) you can use airflow's CLI [test](https://airflow.apache.org/cli.html#test) command:
```bash
$ docker-compose run --rm webserver "" # to execute /bin/bash inside the container
$ airflow test {dag_id} {task_id} {execution_date}
```

For testing tasks with operators that require access to Google Cloud Storage, if connection is forbidden (403) repeatedly, even after authentication, then please run:
```
$ gcloud beta auth application-default login
```
It obtains user access credentials via a web flow and puts them in the well-known location for Application Default Credentials, Details: https://cloud.google.com/sdk/gcloud/reference/beta/auth/application-default/login

### Miscellaneous

#### Running all tests:

The run all command to execute

```shell script
$ ./bin/run-tests.sh -b $AIRFLOW_BUSINESS_UNIT
```

#### Running YAML lint:

Execute the following commands to run YAML linting

```shell script
$ ./bin/run-yamllint.sh -p dags #(lints all in yaml files in dags foler)
$ ./bin/run-yamllint.sh -p src #(lints all in yaml files in src foler)
```

#### Web UI

The web interface can be accessed at [http://localhost:8080](http://localhost:8080) using the following credentials:

```
Username: airflow
Password: airflow
```

#### Run the db migrations:

```bash
$ docker-compose run --rm webserver airflow upgradedb
```

### Testing DAGs using S3

In order to run DAGs locally that use S3, we currently hook your local `~/.aws` directory (which contains your AWS credentials) into the Airflow container - this happens in `docker-compose.yml`.
As of now (`Airflow==1.10.10)`, it is your responsibility to make sure the appropriate AWS profile exists in your AWS credentials and is configured with the correct access, for example `logistics-staging`.
Once we upgrade to `Airflow>=1.10.12` you can dynamically set the AWS profile in the S3 (or more general AWS) Airflow connection via the `session_kwargs` extra field, see https://airflow.apache.org/docs/stable/howto/connection/aws.html#examples-for-the-extra-field .

```json
{
  "session_kwargs": {
    "profile_name": "logistics-staging"
  }
}
```

## Configuration

`dags/${AIRFLOW_BUSINESS_UNIT}/config.yaml` contains the default configuration, it's possible to override it using `Variable` in airflow GUI.
To do that just create a configuration named `configuration` and as a value add the yaml content you want to change:

```yaml
databricks:
  import_data:
    enabled: true

```

## Useful commands

**Note:** All commands bellow use `docker-compose` as an example, but they can also be applied in **production** using.

```bash
$ kubectl exec -n dwh \
    -it $(kubectl get pods -n dwh -l 'app=airflow-web' -o jsonpath='{.items[0].metadata.name}') -- \
    YOUR COMMAND HERE
```

### Cleaning failed tasks

To clean failed tasks in all sub dags within a specific time-frame:

```bash
$ docker-compose run --rm webserver \
   airflow clear -s "2018-07-20" -e "2018-07-26" \
    --dag_regex "import-dwh-v4-\w{2}.merge-layer-\w{2}" \
    --exclude_subdags --downstream --only_failed
```

### Creating variables

```bash
$ docker-compose run --rm webserver \
    airflow variables -s XYZ '{}'
```

### Airflow DB Migrations

To apply DB migrations please run

```bash
$ docker-compose run --rm webserver \
    airflow upgradedb
```

### Delete a dag in the database

```bash
$ docker-compose run --rm webserver \
    delete-dag.sh *dag_name*
```

Where *dag_name* is the name of the DAG, the command will delete all DAG *starting* with *dag_name*

### Run a SQL file

```bash
$ docker-compose run --rm webserver \
    python bin/execute-sql.py -f dwh_import/sql/fct_table.sql -s 2018-01-01 -e 2018-10-01
```


### Creating a dag

Dags to be displayed in the UI should be under the `dags/${AIRFLOW_BUSINESS_UNIT}` folder while any helper code should be in a subfolder.
You should try to make a dag instance idempotent so that is not dependant on the time it is run, only on the parameters
passed to it.  In this case, the `run_date` should configure when data pulls start/end or other env variables.


## Deployment

### Deployment via kube tool

A tagged release can be deployed via kube-tool from [logistics-infra-helm](https://github.com/foodora/logistics-infra-helm)

```bash
$ ./tools/kube_tool deploy chart -p airflow -r eu-west-1 -e {enviroment} -c {business_unit}  -v {version} --verbose
```

where the production enviroment is `production-dwh` and the business_units are: logistics and pandata

### Deployment via Slack

Open the channel **#log-chapter-data** and write: `logot deploy airflow <BRANCH_NAME> to staging eu {buienss_unit}`
where business_units are: log, log-ds and pandata (log-ds only exists for staging)

### Bucket to store logs (used only on staging / production)

A connection with id `s3_logs` must be set in the Airflow connections before logs can be saved.

- Airflow notes on [logging setup](https://github.com/apache/incubator-airflow/blob/master/UPDATING.md#logging-update)
- Also check Stack Overflow answers [here](https://stackoverflow.com/questions/44780736/setting-up-s3-for-logs-in-airflow) and [here](https://stackoverflow.com/questions/48817258/setting-up-s3-logging-in-airflow)

### How to create a Fernet key

Create a Fernet key used to encrypt sensitive data in the database

```bash
$ kubectl exec -n dwh \
    -it $(kubectl get pods -n dwh -l 'app=airflow-web' -o jsonpath='{.items[0].metadata.name}') -- \
    python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)"
```

### Debugging Pods on staging

You can use [Sysdig](https://app.sysdigcloud.com), to monitor the status of the airflow pods
and jobs.
You can also use `kubectl` ([instruction](https://confluence.deliveryhero.com/display/LOG/Kubernetes+First+Setup)) to get logs and status of individual pods in the namespace `-n dwh` (only staging), or ssh into
a running pod to see the local state of the file system.

```bash
kubectl-log-stg -n dwh get pods
kubectl-log-stg -n dwh logs <podname>
kubectl-log-stg -n dwh describe pod <podname>
kubectl-log-stg exec -it <podname> -- /bin/bash
```


## Common errors

* **Problem:** While testing locally `Variable XYZ does not exist` pops up.
**Solution**: Create the the variable via `Admin -> Variables -> Create` or just use the command line:

* **Problem:** Database errors and scheduling errors when airflow starts.
**Solution**: Try updating your airflow dev instance.

* **Problem:** Cannot find your aws config profile in botocore, in ubuntu.
**Solution**: Check the permissions of your aws credentials.

* **Problem:** KeyError and therefore broken DAG when trying to add a pool to an operator via the `pool` parameter.
**Solution**: Add the pool to the global [config.yaml](https://github.com/foodora/datahub-airflow/blob/master/dags/config.yaml).

## Using the Kubernetes Pod Operator

If you want to run a script on a docker image, use the Kubernetes Pod Operator.  To properly configure the operator,
use the following custom operator.

```python
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator
```

Some important default settings that you may want to alter when using this operator

1. node_profile = ['simulator-node', 'batch-node', dwh-node']
    The default for this is `batch-node` which will run on a r4.2xLarge instance reserved instance  This
    only scales up to 5 nodes and should be dedicated to long running jobs since its reserved instance or jobs
    that are not good if they fail, such as staffing.
    `simulator-node` will run on a c4.2xLarge instance spot instance.
    `dwh-node` will run on the general compute infrastructure, also spot instance.
    `gpu-node` will run with a gpu instance

1. is_delete_operator_pod = True
    Will delete the pod once done running the script

1. image_pull_policy = 'Always'
    If you are using latest, keep this setting. If you are running tagged versions could change to 'IfNotPresent'

1. add_conn_id_to_env=None,  #('dwh_reader', 'dwh'),
    Will Try to add these conn ids to the env, with each consecutive one being a fallback if the env doesn't exist as the dwh.


Manually set the resources the pod will take to pass into the `resources`

```python
from airflow.contrib.kubernetes.pod import Resources
pod_resource = Resources(**{
  'request_cpu': "400m",
  'limit_cpu': "400m",
  'request_memory': "1000Mi",
  'limit_memory': "1000Mi"
})
```

If you want custom labels outside the dag and or operator, you can pass in custom labels

    labels={"custom": "custom-label"},

Also make sure that the pod name and task id do not contain
any characters that are not web compliant, no underscores `_`!!

    task_id=f'train-model-hurrier-delay-{country_code}',
    name=f'train-model-hurrier-delay-{country_code}',

## Using the Kubernetes Pod Operator with country-specific resources

For computationally heavy tasks which are run per country it might make sense to use different resources, depending on the country.
For varying resources it makes sense to use airflow variables to store them, as they might be changed regularly.
You can set them up in the following way:

2. add a resources variable to airflow, make sure to flag a default via `is_default`, for ex.:
```json
{
  "default": {
    "request_cpu": "9000m",
    "limit_cpu": "9000m",
    "request_memory": "29000Mi",
    "limit_memory": "29000Mi",
    "is_default": true
    },
  "large": {
    "countries": ["cz", "ar", "hk", "th", "tw", "my"],
    "request_cpu": "15000m",
    "limit_cpu": "15000m",
    "request_memory": "32000Mi",
    "limit_memory": "32000Mi"
    }
}
```

2. define default resources for your dag in the config (and optionally in staging and production) which can be used if the airflow variable is not set (for ex. in dev enviroments):
```yaml
your-dag:
  resources:
    request_cpu: "9000m"
    limit_cpu: "9000m"
    request_memory: "29000Mi"
    limit_memory: "29000Mi"
```

3. parse variable resp. config within dag:
```python
from operators.utils.resources import ResourcesList

resources = Variable.get('your-dag-resources',
                         default_var=config['your-dag']['resources'],
                         deserialize_json=True)
resources_list = ResourcesList(resources)
```

4. use resources in kubernetes operators via the following, which will automatically pick the country-specific resources if defined, otherwise the defaults (alternatively `resources=resources_list.get_default()` or `resources=resources_list.get()` also work):
```python
resources=resources_list.get(country_code)
```


# Common Problems

* **Problem:** Environmental Variables are not being passed correctly or the pod cannot start.
**Solution**: The dictionary of environmental variables passed to `env_vars` must ONLY contain string values!

# Extend the catalog documentation

We use `gitbook` to write the catalog documentation.
The documentation is located in the `catalog` folder.

## GitBook setup

From the [catalog](./catalog) folder:
```shell script
$ npm install
$ npm run gitbook install
```

To test the code:
```shell script
$ npm run serve
```
It will run a nodejs application that renders in real-time the MD files.

To update catalog from BigQuery's metadata, first ensure you have default
credentials authenticated and follow the prompts after.

``` shell
gcloud auth application-default
npm run update_catalog
```

