<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use Broadway\ReadModel\SerializableReadModel;

final class UnexpectedSerializableReadModel implements SerializableReadModel
{
    public static bool $deserializeWasCalled = false;

    public function __construct(
        private readonly string $id
    ) {
    }

    public function getId(): string
    {
        return $this->id;
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
        self::$deserializeWasCalled = true;

        return new self($data['id']);
    }
}
