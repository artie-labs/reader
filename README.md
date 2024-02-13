<div align="center">
  <img height="150px" src="https://github.com/artie-labs/transfer/assets/4412200/238df0c7-6087-4ddc-b83b-24638212af6a"/>
  <h3>Artie Reader</h3>
  <p><b>📚 Perform historical snapshots and read CDC streams from databases 📚</b></p>
  <a href="https://artie.so/slack"><img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack"/></a>
  <a href="https://github.com/artie-labs/reader/blob/master/LICENSE.txt"><img src="https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg"/></a>
  <img src="https://github.com/artie-labs/reader/actions/workflows/gha-go-test.yaml/badge.svg"/>
  <br/>
  <b><a target="_blank" href="https://artie.so" >Learn more »</a></b>
</div>
<br/>

Artie Reader reads from databases to perform historical snapshots and also reads change data capture (CDC) logs for continuous streaming. The generated messages are Debezium capable.

## Benefits
* Historical table snapshots do not require database locks, which means Artie Reader minimizes impact to database performance and avoids situations like replication slot overflow.
* Debezium compatible. The generated messages are consistent with Debezium’s message format.
* Portable and easy to operate. Shipped as a standalone binary with no external dependencies.

## Architecture
<div align="center">
  <img alt="Artie Reader Architecture" src="https://github.com/artie-labs/reader/assets/4412200/d088853a-1e2f-465e-b573-c19ad07e0f04"/>
</div>

## Supports:

|            | Snapshot | Streaming |
|------------|----------|-----------|
| DynamoDB   | ✅        | ✅         |
| MongoDB    | ✅        | ❌         |         
| MySQL      | 🚧       | ❌         |
| PostgreSQL | ✅        | ❌         |


## Running

To get started, you'll need a `config.yaml` file, you can see examples of this in the [examples](https://github.com/artie-labs/reader/tree/master/examples) folder.

```bash
go run main.go --config config.yaml
```
