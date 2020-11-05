# Pandata DAGs


## Structure

We group DAGs by use case.

- [configs](./configs) - Configuration of DAGs with validation to minimize errors.
- [load](./load) - DAGs that load from an external source with as little transformation
  as possible
- [transform](./transform) - DAGs that transform data that have been loaded to the
  data warehouse
- [monitor](./monitor) - DAGs for checks or maintenance
- [utils](./utils) - Anything that isn't constant which used throughout this repository
- [constants](./constants) - Any constants that are used throughout this repository


## Local Setup

After following the steps in the main [README.md](../../README.md),
fill up the **Project Id** to be `dhh---analytics-apac-staging`.


## FAQs

<details>
  <summary>How can I create a new dataset?</summary>
  <p>

Add a new `BigQueryDataset` object to the `BigQueryConfig` in
[staging](./configs/bigquery/datasets/staging.py) and
[production](./configs/bigquery/datasets/production.py).
And ensure that the right access is given.

  </p>
</details>

<details>
  <summary>How can I create a new table?</summary>
  <p>

Find the DAG that creates the table you're looking for. Create
an SQL file that has the same name as the table.
Find the specific table configuration in this [directory](./configs/bigquery/tables)
and add the table that you'd want to create.
Don't forget to also add a test to dry run the SQL file
to ensure that it is valid.

  </p>
</details>

<details>
  <summary>How can I update a table description or field description?</summary>
  <p>

Find the specific table configuration in this [directory](./configs/bigquery/tables)
and update the description of the fields or table.

  </p>
</details>


## Testing

Other than unit tests running in `pytest`, we also dry run our queries
in the staging GCP project to ensure they are valid.

For authentication in Travis, we encode in base64 the json file
that is required to add a GCP connection using the Airflow CLI and
store it in an environment variable.

```javascript
{
	"extra__google_cloud_platform__keyfile_dict": "<ESCAPED_SERVICE_KEY_JSON>",
	"extra__google_cloud_platform_project": "dhh---analytics-apac-admin",
	"extra__google_cloud_platform_scope": "https://www.googleapis.com/auth/bigquery"
}
```
