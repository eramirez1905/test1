const markdownTableHeading = `| Name | Type | Description |
| :--- | :--- | :---        |
`;

const timePartitioningText = (metadata) => {
  return `
Partitioned by field \`${metadata.timePartitioning.field}\` with each
partition being a time interval of \`${metadata.timePartitioning.type}\`
`;
};

const clusteringText = (metadata) => {
  const clusteringFields = metadata.clustering.fields;
  let text = `
Clustered by field \`${clusteringFields.join(', ')}\`
`;
  if (clusteringFields.length > 1) {
    text = `
Clustered by fields \`${clusteringFields.join('`, `')}\`
`;
  }
  return text;
};

const tableMetadataToMarkdown = (metadata) => {
  let mdText = `# ${metadata.tableReference.tableId}

${metadata.description}
`;
  if (metadata.timePartitioning) {
    mdText += timePartitioningText(metadata);
  }

  if (metadata.clustering) {
    mdText += clusteringText(metadata);
  }
  return mdText;
};

const addAnchorURL = (mdText) => {
  const cleanAnchorURL = mdText.split('_').join('');
  return `[${mdText}](#${cleanAnchorURL})`;
};

const isRecord = (schemaField) => {
  return schemaField.type === 'RECORD';
};

const isArray = (schemaField) => {
  return schemaField.mode === 'REPEATED';
};

const addH2Markdown = (mdText) => {
  return `## ${mdText}\n`;
};

const schemaFieldToMarkdownTableRow = (schemaField) => {
  let name = schemaField.name;
  let type = schemaField.type;
  const description = schemaField.description || '';
  if (isArray(schemaField)) {
    type = `ARRAY<${type}>`;
  }
  if (isRecord(schemaField)) {
    name = addAnchorURL(mdText=name);
  };
  return `| ${name} | \`${type}\` | ${description} |`;
};

const currentSchemaFieldsToMarkdown = (schemaFields) => {
  let markdownStr = '';
  let i = 0;
  while (i < schemaFields.length) {
    const row = schemaFieldToMarkdownTableRow(schemaFields[i]);
    markdownStr += `${row}\n`;
    i++;
  }
  return markdownStr;
};

const schemaFieldsToMarkdown = (schemaFields, heading = '') => {
  let markdownStr = '';
  let i = 0;
  markdownStr += `\n${heading}\n${markdownTableHeading}`;
  markdownStr += currentSchemaFieldsToMarkdown(schemaFields);
  while (i < schemaFields.length) {
    if (isRecord(schemaFields[i])) {
      const subheading = addH2Markdown(mdText=schemaFields[i].name);
      const childFields = schemaFields[i].fields;
      markdownStr += schemaFieldsToMarkdown(childFields, subheading);
    };
    i++;
  }
  return markdownStr;
};

const parse = (metadata) => {
  let markdownText = tableMetadataToMarkdown(metadata);
  markdownText += schemaFieldsToMarkdown(metadata.schema.fields);
  return markdownText;
};


module.exports = parse;
