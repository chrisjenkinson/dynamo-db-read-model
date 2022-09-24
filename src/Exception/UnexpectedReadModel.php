<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Exception;

use OutOfBoundsException;

final class UnexpectedReadModel extends OutOfBoundsException
{
    public function __construct(string $actual, string $expected)
    {
        parent::__construct(sprintf('Mismatch between data (%s) and expected class (%s)', $actual, $expected));
    }
}
