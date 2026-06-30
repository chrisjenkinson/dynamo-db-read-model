<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use Broadway\ReadModel\Identifiable;

final class DeferredOperation
{
    public const REMOVE = 'remove';

    public const SAVE = 'save';

    public function __construct(
        public readonly string $operation,
        public readonly string $id,
        public readonly ?Identifiable $model = null
    ) {
    }
}
