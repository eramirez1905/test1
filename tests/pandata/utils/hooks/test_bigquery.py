import pytest


from configs.constructors.table import Field
from utils.hooks.bigquery import BigQueryPatchTableHook


class TestBigQueryPatchTableHook:
    hook = BigQueryPatchTableHook()

    @pytest.mark.parametrize(
        "schema,field_configs,expected",
        [
            pytest.param(
                {
                    "fields": [
                        {"name": "normal_field", "type": "INTEGER", "mode": "NULLABLE"},
                    ]
                },
                (Field(name="normal_field"),),
                {
                    "fields": [
                        {
                            "name": "normal_field",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                        },
                    ]
                },
                id="empty_description",
            ),
            pytest.param(
                {
                    "fields": [
                        {"name": "normal_field", "type": "INTEGER", "mode": "NULLABLE"},
                    ]
                },
                (Field(name="normal_field", description="foo"),),
                {
                    "fields": [
                        {
                            "name": "normal_field",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                            "description": "foo",
                        },
                    ]
                },
                id="basic_one_field",
            ),
            pytest.param(
                {
                    "fields": [
                        {
                            "name": "normal_field",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                            "fields": [
                                {"name": "child", "type": "STRING", "mode": "NULLABLE"}
                            ],
                        },
                    ]
                },
                (
                    Field(
                        name="normal_field",
                        description="foo",
                        child_fields=(Field(name="child", description="i am a child"),),
                    ),
                ),
                {
                    "fields": [
                        {
                            "name": "normal_field",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                            "description": "foo",
                            "fields": [
                                {
                                    "name": "child",
                                    "type": "STRING",
                                    "mode": "NULLABLE",
                                    "description": "i am a child",
                                }
                            ],
                        },
                    ]
                },
                id="nested_one_level",
            ),
            pytest.param(
                {
                    "fields": [
                        {
                            "name": "normal_field",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                            "fields": [
                                {
                                    "name": "child",
                                    "type": "STRING",
                                    "mode": "NULLABLE",
                                    "fields": [
                                        {
                                            "name": "child_child",
                                            "type": "STRING",
                                            "mode": "NULLABLE",
                                        }
                                    ],
                                }
                            ],
                        },
                    ]
                },
                (
                    Field(
                        name="normal_field",
                        description="foo",
                        child_fields=(
                            Field(
                                name="child",
                                description="i am a child",
                                child_fields=(
                                    Field(
                                        name="child_child",
                                        description="i am child's child",
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                {
                    "fields": [
                        {
                            "name": "normal_field",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                            "description": "foo",
                            "fields": [
                                {
                                    "name": "child",
                                    "type": "STRING",
                                    "mode": "NULLABLE",
                                    "description": "i am a child",
                                    "fields": [
                                        {
                                            "name": "child_child",
                                            "type": "STRING",
                                            "mode": "NULLABLE",
                                            "description": "i am child's child",
                                        }
                                    ],
                                }
                            ],
                        },
                    ]
                },
                id="nested_two_levels",
            ),
        ],
    )
    def test_patch_schema(self, schema, field_configs, expected):
        patched_schema = self.hook.patch_schema(fields=field_configs, schema=schema)
        assert patched_schema == expected

    @pytest.mark.parametrize(
        "to_update,update,expected",
        [
            pytest.param(
                [{"name": "normal_field", "type": "INTEGER", "mode": "NULLABLE"}],
                [{"name": "normal_field", "description": None}],
                [{"name": "normal_field", "type": "INTEGER", "mode": "NULLABLE"}],
                id="empty_description",
            ),
            pytest.param(
                [{"name": "normal_field", "type": "INTEGER", "mode": "NULLABLE"}],
                [{"name": "normal_field"}],
                [{"name": "normal_field", "type": "INTEGER", "mode": "NULLABLE"}],
                id="no_description",
            ),
            pytest.param(
                [{"name": "normal_field", "type": "INTEGER", "mode": "NULLABLE"}],
                [{"name": "normal_field", "description": "blah"}],
                [
                    {
                        "name": "normal_field",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                        "description": "blah",
                    },
                ],
                id="basic_description",
            ),
        ],
    )
    def test_merge_schema_fields_api_repr(self, to_update, update, expected):
        self.hook.merge_schema_fields_api_repr(to_update, update)
        assert to_update == expected
