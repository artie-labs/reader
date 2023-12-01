<div align="center">
  <img height="150px" src="https://github.com/artie-labs/transfer/assets/4412200/238df0c7-6087-4ddc-b83b-24638212af6a"/>
  <h3>Artie Reader</h3>
  <p><b>📚 Grabbing data changes from various sources such as DynamoDB 📚</b></p>
  <a href="https://artie.so/slack"><img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack"/></a>
  <a href="https://github.com/artie-labs/reader/blob/master/LICENSE.txt"><img src="https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg"/></a>
  <img src="https://github.com/artie-labs/reader/actions/workflows/gha-go-test.yaml/badge.svg"/>
  <br/>
  <b><a target="_blank" href="https://artie.so" >Learn more »</a></b>
</div>
<br/>

## Getting this running

Generate a `config.yaml` file with the following contents:

```yaml
dynamodb:
  tableName: tableName
  offsetFile: /tmp/offsets.txt
  awsRegion: us-east-1
  awsAccessKeyId: foo
  awsSecretAccessKey: bar
  streamArn: arn:aws:dynamodb:us-east-1:123456789012:table/tableName/stream/2019-12-20T00:00:00.000

kafka:
  bootstrapServers: localhost:29092
  topicPrefix: topicPrefix
```

Then run the following command:

```bash
go run main.go --config config.yaml
```

## What is currently supported?
* DynamoDB (via DynamoDB streams)
