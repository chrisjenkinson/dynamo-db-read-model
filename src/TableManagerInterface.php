<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

interface TableManagerInterface
{
    public function createTable(): void;

    public function deleteTable(): void;
}
