<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use Broadway\ReadModel\Repository;

interface FlushableRepository extends Repository
{
    /**
     * Flushes pending changes one write at a time. This is not atomic and may
     * leave earlier writes persisted if a later write fails.
     */
    public function flush(): void;

    /**
     * @param array<string, scalar|null> $context
     */
    public function flushWithContext(array $context): void;

    public function clear(): void;

    /**
     * @return DeferredOperation[]
     */
    public function pendingOperations(): array;
}
