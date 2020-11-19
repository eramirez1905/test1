const each = require('jest-each').default;
const parse = require('../../update/parse-md.js');

const tableReference = {
  projectId: 'fulfillment-dwh-production',
  datasetId: 'pandata_curated',
  tableId: 'table_id',
};

const idField = {
  name: 'id',
  type: 'STRING',
  mode: 'NULLABLE',
  description: 'blah',
};

const createdDateField = {
  name: 'created_date_utc',
  type: 'DATE',
  mode: 'NULLABLE',
  description: 'Created date in utc',
};

const defaultMarkdownHeader = `# table_id

this is a description

Partitioned by field \`created_date_utc\` with each
partition being a time interval of \`DAY\`

Clustered by field \`id\`

`;

const defaultMetadata = {
  tableReference: tableReference,
  clustering: {
    fields: ['id'],
  },
  timePartitioning: {
    type: 'DAY',
    field: 'created_date_utc',
  },
  description: 'this is a description',
};

describe('metadata to expected markdown text', () => {
  each([
    [
      {
        tableReference: tableReference,
        description: '',
        schema: {
          fields: [
            idField,
            createdDateField,
          ],
        },
      },
      `# table_id




| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
`,
    ],
    [
      Object.assign(
          {},
          defaultMetadata, {
            schema: {
              fields: [
                idField,
                createdDateField,
              ],
            },
          },
      ),
      `${defaultMarkdownHeader}
| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
`,
    ],
    [
      Object.assign(
          {},
          defaultMetadata, {
            clustering: {
              fields: ['rdbms_id', 'id'],
            },
            schema: {
              fields: [
                idField,
                createdDateField,
              ],
            },
          },
      ),
      `# table_id

this is a description

Partitioned by field \`created_date_utc\` with each
partition being a time interval of \`DAY\`

Clustered by fields \`rdbms_id\`, \`id\`


| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
`,
    ],
    [
      Object.assign(
          {},
          defaultMetadata, {
            schema: {
              fields: [
                idField,
                createdDateField,
                {
                  name: 'parent_with_underscore',
                  type: 'RECORD',
                  mode: 'REPEATED',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child',
                      type: 'STRING',
                      mode: 'NULLABLE',
                      description: 'i am a child',
                    },
                  ],
                },
              ],
            },
          },
      ),
      `${defaultMarkdownHeader}
| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
| [parent_with_underscore](#parentwithunderscore) | \`ARRAY<RECORD>\` | blah |

## parent_with_underscore

| Name | Type | Description |
| :--- | :--- | :---        |
| child | \`STRING\` | i am a child |
`,
    ],
    [
      Object.assign(
          {},
          defaultMetadata, {
            schema: {
              fields: [
                idField,
                createdDateField,
                {
                  name: 'parent',
                  type: 'RECORD',
                  mode: 'REPEATED',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child',
                      type: 'STRING',
                      mode: 'NULLABLE',
                      description: 'i am a child',
                    },
                  ],
                },
              ],
            },
          },
      ),
      `${defaultMarkdownHeader}
| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
| [parent](#parent) | \`ARRAY<RECORD>\` | blah |

## parent

| Name | Type | Description |
| :--- | :--- | :---        |
| child | \`STRING\` | i am a child |
`,
    ],
    [
      Object.assign(
          {},
          defaultMetadata, {
            schema: {
              fields: [
                idField,
                createdDateField,
                {
                  name: 'parent',
                  type: 'RECORD',
                  mode: 'REPEATED',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child',
                      type: 'STRING',
                      mode: 'NULLABLE',
                      description: 'i am a child',
                    },
                  ],
                },
                {
                  name: 'parent2',
                  type: 'RECORD',
                  mode: 'REPEATED',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child2',
                      type: 'STRING',
                      mode: 'NULLABLE',
                      description: 'i am a child',
                    },
                  ],
                },
              ],
            },
          },
      ),
      `${defaultMarkdownHeader}
| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
| [parent](#parent) | \`ARRAY<RECORD>\` | blah |
| [parent2](#parent2) | \`ARRAY<RECORD>\` | blah |

## parent

| Name | Type | Description |
| :--- | :--- | :---        |
| child | \`STRING\` | i am a child |

## parent2

| Name | Type | Description |
| :--- | :--- | :---        |
| child2 | \`STRING\` | i am a child |
`,
    ],
    [
      Object.assign(
          {},
          defaultMetadata, {
            schema: {
              fields: [
                idField,
                createdDateField,
                {
                  name: 'parent',
                  type: 'RECORD',
                  mode: 'REPEATED',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child',
                      type: 'RECORD',
                      mode: 'REPEATED',
                      description: 'i am a child',
                      fields: [
                        {
                          name: 'child_child',
                          type: 'STRING',
                          mode: 'NULLABLE',
                          description: 'i am a child child',
                        },
                      ],
                    },
                  ],
                },
                {
                  name: 'parent2',
                  type: 'RECORD',
                  mode: 'REPEATED',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child2',
                      type: 'STRING',
                      mode: 'NULLABLE',
                      description: 'i am a child',
                    },
                  ],
                },
              ],
            },
          },
      ),
      `${defaultMarkdownHeader}
| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
| [parent](#parent) | \`ARRAY<RECORD>\` | blah |
| [parent2](#parent2) | \`ARRAY<RECORD>\` | blah |

## parent

| Name | Type | Description |
| :--- | :--- | :---        |
| [child](#child) | \`ARRAY<RECORD>\` | i am a child |

## child

| Name | Type | Description |
| :--- | :--- | :---        |
| child_child | \`STRING\` | i am a child child |

## parent2

| Name | Type | Description |
| :--- | :--- | :---        |
| child2 | \`STRING\` | i am a child |
`,
    ],
    [
      Object.assign(
          {},
          defaultMetadata, {
            schema: {
              fields: [
                idField,
                createdDateField,
                {
                  name: 'parent',
                  type: 'RECORD',
                  mode: 'NULLABLE',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child',
                      type: 'RECORD',
                      mode: 'REPEATED',
                      description: 'i am a child',
                      fields: [
                        {
                          name: 'child_child',
                          type: 'STRING',
                          mode: 'NULLABLE',
                          description: 'i am a child child',
                        },
                      ],
                    },
                  ],
                },
                {
                  name: 'parent2',
                  type: 'RECORD',
                  mode: 'REPEATED',
                  description: 'blah',
                  fields: [
                    {
                      name: 'child2',
                      type: 'STRING',
                      mode: 'NULLABLE',
                      description: 'i am a child',
                    },
                  ],
                },
              ],
            },
          },
      ),
      `${defaultMarkdownHeader}
| Name | Type | Description |
| :--- | :--- | :---        |
| id | \`STRING\` | blah |
| created_date_utc | \`DATE\` | Created date in utc |
| [parent](#parent) | \`RECORD\` | blah |
| [parent2](#parent2) | \`ARRAY<RECORD>\` | blah |

## parent

| Name | Type | Description |
| :--- | :--- | :---        |
| [child](#child) | \`ARRAY<RECORD>\` | i am a child |

## child

| Name | Type | Description |
| :--- | :--- | :---        |
| child_child | \`STRING\` | i am a child child |

## parent2

| Name | Type | Description |
| :--- | :--- | :---        |
| child2 | \`STRING\` | i am a child |
`,
    ],
  ]).it('when the metadata is \'%s\'', (metadata, expected) => {
    expect(parse(metadata)).toBe(expected);
  });
});
