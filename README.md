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

The DynamoDB `Id` value is required to match the serialized read model's
`Identifiable::getId()`. Rows where the physical key and payload id differ are
treated as invalid stored data and rejected on read; repair those rows with
explicit operational tooling rather than relying on repository normalization.

`find($id)` uses the full primary key (`Name` + `Id`) and is the bounded lookup
to prefer for production read paths. `findAll()` queries the full repository
partition for the configured `Name` and returns every read model in memory.
`findBy()` also queries the full repository partition, deserializes each read
model, and then filters in PHP. For production-sized collections, avoid using
`findAll()` or `findBy()` on request paths unless that full-partition work is
intentional.

## Snapshot Store

This release changes the public constructors for `DynamoDbRepositoryFactory`
and `DynamoDbRepository`.

`DynamoDbRepositoryFactory` requires a `ReadModelSnapshotStore` so repositories
created by the same factory can suppress unchanged physical writes without an
extra DynamoDB read:

```php
$factory = new DynamoDbRepositoryFactory($client, $serializer, 'read-models', new ReadModelSnapshotStore());
```

The factory is the recommended construction path. It creates the per-repository
storage and matcher internally.

Direct `DynamoDbRepository` construction is still possible, but its constructor
now expects explicit storage and matcher collaborators. If you manually construct
repositories, move the DynamoDB-specific arguments into a
`DynamoDbReadModelStorage` configured for that table, repository name, and read
model class, then pass that storage with a `ReadModelFieldMatcher`:

```php
$storage = new DynamoDbReadModelStorage(
    $client,
    $inputBuilder,
    $serializer,
    $jsonEncoder,
    $jsonDecoder,
    'read-models',
    'repository-name',
    MyReadModel::class,
    new ReadModelSnapshotStore()
);

$repository = new DynamoDbRepository(
    $storage,
    new ReadModelFieldMatcher()
);
```

Call `$factory->clearSnapshots()` before reusing the same factory after deleting
or recreating the backing read-model table.

## Deferred Persistence

Repositories created with `DynamoDbRepositoryFactory::create()` keep the original
immediate behaviour: `save()` writes with `PutItem`, `remove()` writes with
`DeleteItem`, and `find()` reads with `GetItem`.

For replay or other batching-sensitive workflows, opt in explicitly to deferred
persistence:

```php
$repository = $factory->createDeferred('repository-name', MyReadModel::class);

$model = $repository->find($id) ?? new MyReadModel($id);

// Projector handlers can keep using Broadway\Repository methods.
$repository->save($model);

// Write pending removals and saves to DynamoDB.
$repository->flush();
```

`createDeferred()` is package-specific and is not part of Broadway's
`RepositoryFactory` interface. Code that needs deferred repository creation
should type-hint `DeferredRepositoryFactory` or the concrete
`DynamoDbRepositoryFactory`.

A deferred repository still implements `Broadway\ReadModel\Repository`, and also
implements `FlushableRepository`:

```php
interface FlushableRepository extends Broadway\ReadModel\Repository
{
    public function flush(): void;

    public function flushWithContext(array $context): void;

    public function clear(): void;

    public function pendingOperations(): array;
}
```

In deferred mode, `find($id)` checks an in-memory identity map before DynamoDB.
`save()` captures the read model's serialized state at the time `save()` is
called and stages that state in memory. Repeated saves for the same read-model id
replace the staged state and collapse to one `PutItem` on `flush()`. Mutating a
read model after `save()` does not change the staged write unless `save()` is
called again. `remove()` marks the id removed in memory, so `find($id)` returns
`null` until the item is saved again or `clear()` discards the deferred state.
Saving a removed id cancels the pending delete.

`pendingOperations()` returns the currently staged removes and save-time
snapshots for inspection. `flush()` writes pending deletes and saves one item at
a time through the same DynamoDB storage path as the immediate repository. If a
write fails, the failing and remaining pending state is retained for retry or
inspection. Successfully flushed entries are no longer pending. `clear()` only
discards local managed, dirty, and removed state; it does not write anything.

Failures are reported with `DeferredFlushFailed`, which includes the operation,
read-model id, table, repository name, read-model class, and previous exception.
The repository does not know application-level details such as tenant id,
projector name, or source event id. Pass those at the flush boundary when they
are useful:

```php
$repository->flushWithContext([
    'tenantId' => $tenantId,
    'projector' => SomeProjector::class,
    'sourceEventId' => $eventId,
]);
```

`findAll()` and `findBy()` are merge-aware: they query DynamoDB and overlay
pending local saves/removes before returning results. They still perform a
DynamoDB query each time, and DynamoDB reads use the repository's normal
consistency settings. Deferred mode makes this repository's staged changes
visible; it does not make broad projection queries transactional or globally
fresh.

`findBy()` remains available for Broadway compatibility and small read-side
collections. Do not use it as a write-side invariant or duplicate guard during
command handling, seeding, or projection rebuilds. Model those cases as
deterministic lookup read models and query them with `find($id)`.

The identity map is scoped to the deferred repository instance. It can return a
cached model even if another process changes DynamoDB while the deferred
repository is still open. Keep deferred repositories short-lived around a replay,
seed, or console unit of work, and call `clear()` before reusing one across
independent work. For long replays or seeds, use explicit `flush(); clear();`
checkpoints when managed objects are no longer needed.

This is a batching and performance boundary, not a DynamoDB transaction. A failed
flush can leave earlier operations persisted and later operations still pending.

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
