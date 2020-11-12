def add_upsert_view_ddl(  # noqa: E302
    project_name: str, dataset_name: str, view_name: str, sql: str
) -> str:
    UPSERT_VIEW_DDL = "CREATE OR REPLACE VIEW"
    return f"{UPSERT_VIEW_DDL} {project_name}.{dataset_name}.{view_name} AS\n{sql}"
