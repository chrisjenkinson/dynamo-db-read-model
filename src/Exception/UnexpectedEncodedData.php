<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Exception;

use OutOfBoundsException;

final class UnexpectedEncodedData extends OutOfBoundsException
{
    public function __construct(string $data, string $expectedType, mixed $result)
    {
        parent::__construct(sprintf('Expected "%s" to be "%s", instead got "%s".', $data, $expectedType, get_debug_type($result)));
    }
}
