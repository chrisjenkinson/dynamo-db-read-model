<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use Broadway\Serializer\Serializable;

final class NonIdentifiableSerializableReadModel implements Serializable
{
    public function __construct(
        private readonly string $id
    ) {
    }

    /**
     * @return array<string, string>
     */
    public function serialize(): array
    {
        return [
            'id' => $this->id,
        ];
    }

    /**
     * @param array<string, string> $data
     */
    public static function deserialize(array $data)
    {
        return new self($data['id']);
    }
}
