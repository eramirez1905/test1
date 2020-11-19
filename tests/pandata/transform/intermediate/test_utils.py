from transform.intermediate.utils import add_upsert_view_ddl


def test_add_upsert_view_ddl():
    sql = "SELECT * FROM foo.buzz.blah"
    project_name = "project"
    dataset_name = "dataset"
    view_name = "example"
    expected = """CREATE OR REPLACE VIEW {project}.{dataset}.{view} AS
SELECT * FROM foo.buzz.blah""".format(
        project=project_name, dataset=dataset_name, view=view_name
    )
    assert (
        add_upsert_view_ddl(
            project_name=project_name,
            dataset_name=dataset_name,
            view_name=view_name,
            sql=sql,
        )
        == expected
    )
