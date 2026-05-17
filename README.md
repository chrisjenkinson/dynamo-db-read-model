# DynamoDB Read Model

A DynamoDB-backed Broadway read model repository.

## Installation

```bash
composer require chrisjenkinson/dynamo-db-read-model
```

The package supports `async-aws/dynamo-db` `^1.3`, `^2.0`, and `^3.0`.

## DynamoDB Table

Create one table for the read model store. The table uses a composite primary
key:

| Attribute | Type | Key | Description |
| --- | --- | --- | --- |
| `Name` | String | Partition | Repository name passed to `RepositoryFactory::create()` |
| `Id` | String | Sort | Read model identifier from `Identifiable::getId()` |
| `Data` | String | - | JSON-encoded serialized read model payload |

The library queries by `Name` to load all read models for a repository, and uses
`Name` + `Id` for single-item reads, writes, and deletes. It does not require
secondary indexes.

`find($id)` uses the full primary key (`Name` + `Id`) and is the bounded lookup
to prefer for production read paths. `findAll()` queries the full repository
partition for the configured `Name` and returns every read model in memory.
`findBy()` also queries the full repository partition, deserializes each read
model, and then filters in PHP. For production-sized collections, avoid using
`findAll()` or `findBy()` on request paths unless that full-partition work is
intentional.

Example AWS CLI setup:

```bash
aws dynamodb create-table \
  --table-name read-models \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions \
    AttributeName=Name,AttributeType=S \
    AttributeName=Id,AttributeType=S \
  --key-schema \
    AttributeName=Name,KeyType=HASH \
    AttributeName=Id,KeyType=RANGE
```

## Testing

The test suite expects DynamoDB Local. In GitHub Actions this is provided as a
service named `dynamodb-local`; locally, override the endpoint:

```bash
DYNAMODB_ENDPOINT=http://127.0.0.1:8000 \
AWS_ACCESS_KEY_ID=none \
AWS_SECRET_ACCESS_KEY=none \
composer run-script phpunit
```

CI tests supported PHP versions against each supported AsyncAws DynamoDB major.

## Quality Checks

```bash
vendor/bin/ecs check --config ecs.php
composer run-script phpstan
composer run-script infection
```
