<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

/**
 * @internal
 */
final class RepositoryId
{
    public static function normalize(mixed $id): string
    {
        if (null === $id || is_scalar($id) || $id instanceof \Stringable) {
            return (string) $id;
        }

        throw new \InvalidArgumentException(sprintf(
            'Repository ID must be scalar, null, or Stringable; got %s.',
            get_debug_type($id)
        ));
    }
}
