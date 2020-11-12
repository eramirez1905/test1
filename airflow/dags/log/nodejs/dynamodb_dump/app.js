const AWS = require("aws-sdk");
const Backup = require("dynamo-backup-streams").Backup;
const {Transform} = require("stream");
const Parser = require("newline-json").Parser;
const Stringifier = require("newline-json").Stringifier;
const argv = require("minimist")(process.argv.slice(2));
const uploadStream = require("s3-stream-upload");

const tableName = argv["table_name"];
const bucketName = argv["bucket_name"];

const backupStream = new Backup({
    client: new AWS.DynamoDB(),
    table: tableName,
    capacityPercentage: 100,
    fixedCapacity: 40000,
    concurrency: 4,
    delay: 200
});

const unmarshall = new Transform({
    readableObjectMode: true,
    writableObjectMode: true,
    transform(chunk, encoding, callback) {
        const data = AWS.DynamoDB.Converter.unmarshall(chunk);
        this.push(data);
        callback();
    }
});

backupStream
    .pipe(new Parser())
    .pipe(unmarshall)
    .pipe(new Stringifier())
    .pipe(uploadStream(new AWS.S3(), { Bucket: bucketName, Key: `${tableName}.json` }))
    .on("chunk-uploaded", function (chunksUploaded) {
        console.log(chunksUploaded);
    })
    .on("error", function (err) {
        console.error(err);
    })
    .on("finish", function () {
        console.log(`File ${tableName}.json uploaded`);
    });
