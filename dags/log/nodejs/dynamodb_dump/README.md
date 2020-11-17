# Dump DynamoDB table

Install dependencies:

```bash
yarn install
```

Usage:

```bash
AWS_REGION=eu-west-1 node app.js --table_name <dynamoDB table name> --bucket_name <bucket_name>
```

The app will store a file named `<dynamoDB table name>.sql.gz` on S3 `<bucket_name>`
