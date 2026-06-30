<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use AsyncAws\DynamoDb\DynamoDbClient;
use Broadway\ReadModel\Repository;
use Broadway\ReadModel\RepositoryFactory;
use Broadway\Serializer\Serializer;

final class DynamoDbRepositoryFactory implements RepositoryFactory
{
    public function __construct(
        private readonly DynamoDbClient $client,
        private readonly Serializer $serializer,
        private readonly string $table,
        private readonly ReadModelSnapshotStore $snapshots
    ) {
    }

    public function create(string $name, string $class): Repository
    {
        return new DynamoDbRepository($this->createStorage($name, $class), new ReadModelFieldMatcher());
    }

    public function createDeferred(string $name, string $class): FlushableRepository
    {
        return new DeferredDynamoDbRepository($this->createStorage($name, $class), new ReadModelFieldMatcher(), $this->table, $name, $class);
    }

    public function clearSnapshots(): void
    {
        $this->snapshots->clear();
    }

    private function createStorage(string $name, string $class): DynamoDbReadModelStorage
    {
        return new DynamoDbReadModelStorage(
            $this->client,
            new InputBuilder(),
            $this->serializer,
            new JsonEncoder(),
            new JsonDecoder(),
            $this->table,
            $name,
            $class,
            $this->snapshots
        );
    }
}
