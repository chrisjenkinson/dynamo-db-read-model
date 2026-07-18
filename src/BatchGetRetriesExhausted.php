<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use RuntimeException;

final class BatchGetRetriesExhausted extends RuntimeException
{
    /**
     * @param list<string> $unresolvedIds
     */
    public function __construct(
        public readonly string $table,
        public readonly string $repositoryName,
        public readonly string $readModelClass,
        public readonly array $unresolvedIds,
        public readonly int $attempts
    ) {
        parent::__construct(sprintf(
            'Batch get retries exhausted for repository "%s" in table "%s" after %d attempts; unresolved ids: %s.',
            $repositoryName,
            $table,
            $attempts,
            implode(', ', $unresolvedIds)
        ));
    }
}
