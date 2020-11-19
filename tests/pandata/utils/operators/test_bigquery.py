from utils.operators.bigquery import BigQueryPatchTableOperator


class TestBigQueryPatchTableOperator:
    def test_defaults(self):
        operator = BigQueryPatchTableOperator(
            task_id="task",
            gcp_conn_id="foo",
            project_id="project",
            dataset_id="dataset",
            table_id="table",
        )
        assert operator.description is None
        assert operator.fields is None
        assert operator.labels is None
        assert operator.delegate_to is None
        assert operator.is_partition_filter_required is False
