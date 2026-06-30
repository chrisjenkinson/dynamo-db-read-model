<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

interface DeferredRepositoryFactory
{
    public function createDeferred(string $name, string $class): FlushableRepository;
}
