<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

final class DeferredFlushFailed extends \RuntimeException
{
    /**
     * @param array<string, scalar|null> $context
     */
    public function __construct(
        public readonly string $operation,
        public readonly string $id,
        public readonly ?string $table,
        public readonly ?string $repositoryName,
        public readonly ?string $readModelClass,
        public readonly array $context,
        \Throwable $previous
    ) {
        parent::__construct(sprintf(
            'Deferred read-model flush failed while performing %s for id "%s".',
            $operation,
            $id
        ), 0, $previous);
    }
}
