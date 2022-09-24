<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Exception;

use OutOfBoundsException;

final class CannotFilterWithNonexistentKey extends OutOfBoundsException
{
    public function __construct(string $model, string $key)
    {
        parent::__construct(sprintf('No public method or public property "%s" exists for model "%s"', $key, $model));
    }
}
