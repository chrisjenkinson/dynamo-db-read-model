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
        private readonly string $table
    ) {
    }

    public function create(string $name, string $class): Repository
    {
        return new DynamoDbRepository($this->client, new InputBuilder(), $this->serializer, new JsonEncoder(), new JsonDecoder(), $this->table, $name, $class);
    }
}
