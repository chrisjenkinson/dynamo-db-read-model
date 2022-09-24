<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

final class JsonDecoder
{
    /**
     * @return array<mixed>
     */
    public function decode(string $json): array
    {
        return json_decode($json, flags: JSON_THROW_ON_ERROR | JSON_OBJECT_AS_ARRAY);
    }
}
