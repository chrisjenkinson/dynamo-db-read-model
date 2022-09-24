<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

final class JsonEncoder
{
    /**
     * @param array<mixed> $value
     */
    public function encode(array $value): string
    {
        return json_encode($value, flags: JSON_THROW_ON_ERROR);
    }
}
