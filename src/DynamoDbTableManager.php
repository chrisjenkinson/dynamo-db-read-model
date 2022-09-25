<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use AsyncAws\DynamoDb\DynamoDbClient;

final class DynamoDbTableManager implements TableManagerInterface
{
    public function __construct(
        private readonly DynamoDbClient $client,
        private readonly InputBuilder $inputBuilder,
        private readonly string $table
    ) {
    }

    public function deleteTable(): void
    {
        $result = $this->client->tableNotExists($this->inputBuilder->buildDescribeTableInput($this->table));
        $result->resolve();

        if ($result->isSuccess()) {
            return;
        }

        $this->client->deleteTable($this->inputBuilder->buildDeleteTableInput($this->table));
    }

    public function createTable(): void
    {
        $this->client->createTable($this->inputBuilder->buildCreateTableInput($this->table));

        $this->client->tableExists($this->inputBuilder->buildDescribeTableInput($this->table))->wait();
    }
}
