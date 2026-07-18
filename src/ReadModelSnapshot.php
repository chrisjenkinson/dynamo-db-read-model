<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

final class ReadModelSnapshot
{
    /**
     * @param array<mixed> $serializedData
     */
    public function __construct(
        public readonly string $table,
        public readonly string $name,
        public readonly string $class,
        public readonly string $id,
        public readonly array $serializedData
    ) {
    }

    /**
     * @param array<mixed> $serializedData
     */
    public function matches(array $serializedData): bool
    {
        return $this->serializedData === $serializedData;
    }

    public function key(): string
    {
        return self::keyFor($this->table, $this->name, $this->class, $this->id);
    }

    public static function keyFor(string $table, string $name, string $class, string $id): string
    {
        return json_encode([$table, $name, $class, $id], JSON_THROW_ON_ERROR);
    }
}
