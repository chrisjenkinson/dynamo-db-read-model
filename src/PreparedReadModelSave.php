<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

final class PreparedReadModelSave
{
    /**
     * @param array<string, mixed> $serializedData
     */
    public function __construct(
        public readonly string $id,
        public readonly array $serializedData
    ) {
    }
}
