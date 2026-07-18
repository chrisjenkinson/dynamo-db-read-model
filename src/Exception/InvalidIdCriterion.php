<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Exception;

final class InvalidIdCriterion extends \InvalidArgumentException
{
    private function __construct(
        public readonly string $reason,
        public readonly ?int $index,
        public readonly string $actualType,
        string $message
    ) {
        parent::__construct($message);
    }

    public static function nonList(): self
    {
        return new self(
            'non-list',
            null,
            'array',
            'The ID criterion array must be a list.'
        );
    }

    public static function invalidEntry(int $index, mixed $value): self
    {
        $actualType = get_debug_type($value);

        return new self(
            'invalid-entry',
            $index,
            $actualType,
            sprintf('The ID criterion entry at index %d must be a non-empty string; got %s.', $index, $actualType)
        );
    }
}
