<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use chrisjenkinson\DynamoDbReadModel\RepositoryId;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class RepositoryIdTest extends TestCase
{
    public function test_it_preserves_broadway_string_conversion_for_supported_ids(): void
    {
        $stringable = new class() implements \Stringable {
            public function __toString(): string
            {
                return 'stringable';
            }
        };

        self::assertSame('id', RepositoryId::normalize('id'));
        self::assertSame('123', RepositoryId::normalize(123));
        self::assertSame('1.5', RepositoryId::normalize(1.5));
        self::assertSame('1', RepositoryId::normalize(true));
        self::assertSame('', RepositoryId::normalize(false));
        self::assertSame('', RepositoryId::normalize(null));
        self::assertSame('stringable', RepositoryId::normalize($stringable));
    }

    public function test_it_rejects_ids_that_php_cannot_safely_convert_to_strings(): void
    {
        $resource = fopen('php://memory', 'rb');
        self::assertIsResource($resource);

        try {
            foreach ([[1], new \stdClass(), $resource] as $id) {
                try {
                    RepositoryId::normalize($id);
                    self::fail('Expected an invalid repository ID exception.');
                } catch (InvalidArgumentException $exception) {
                    self::assertSame(
                        sprintf('Repository ID must be scalar, null, or Stringable; got %s.', get_debug_type($id)),
                        $exception->getMessage()
                    );
                }
            }
        } finally {
            fclose($resource);
        }
    }
}
