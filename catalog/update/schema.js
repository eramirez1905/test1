const {BigQuery} = require('@google-cloud/bigquery');
const readline = require('readline');
const fs = require('fs');
const parse = require('./parse-md.js');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const projectId = 'fulfillment-dwh-production';
const bigquery = new BigQuery({
  projectId: projectId,
});

const tableToMarkdown = async (table, businessUnit) => {
  table.getMetadata().then((data) => {
    const metadata = data[0];
    const markdownText = parse(metadata);
    const fp = (`./curated_data/${businessUnit}/` +
      `${metadata.tableReference.tableId}.md`
    );
    fs.writeFile(fp, markdownText, (err) => {
      console.log(`Catalog updated at ${fp}`);
    });
  }).catch((reason) => {
    console.error(reason);
  });
};

rl.question('What is the name of your dataset? ', (datasetId) => {
  rl.question('What is your business unit? ', (businessUnit) => {
    const dataset = bigquery.dataset(datasetId);
    dataset.getTables((err, tables) => {});
    dataset.getTables().then((data) => {
      const tables = data[0];
      for (const i in tables) {
        if (tables.hasOwnProperty(i)) {
          tableToMarkdown(tables[i], businessUnit);
        }
      }
    }).catch((reason) => {
      console.error(reason);
    });
    rl.close();
  });
});
