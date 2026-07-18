<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use chrisjenkinson\DynamoDbReadModel\Exception\InvalidIdCriterion;
use chrisjenkinson\DynamoDbReadModel\FindByCriteria;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class FindByCriteriaTest extends TestCase
{
    public function test_it_classifies_supported_id_criteria(): void
    {
        $absent = FindByCriteria::from([
            'name' => 'Alice',
        ]);

        self::assertFalse($absent->hasId);
        self::assertFalse($absent->multiple);
        self::assertFalse($absent->impossible);
        self::assertSame([], $absent->ids);
        self::assertSame([
            'name' => 'Alice',
        ], $absent->remainingFields);

        $uppercaseOnly = FindByCriteria::from([
            'ID' => 'one',
        ]);

        self::assertFalse($uppercaseOnly->hasId);
        self::assertFalse($uppercaseOnly->multiple);
        self::assertFalse($uppercaseOnly->impossible);
        self::assertSame([], $uppercaseOnly->ids);
        self::assertSame([
            'ID' => 'one',
        ], $uppercaseOnly->remainingFields);

        $scalar = FindByCriteria::from([
            'id'   => 'one',
            'name' => 'Alice',
        ]);

        self::assertTrue($scalar->hasId);
        self::assertFalse($scalar->multiple);
        self::assertFalse($scalar->impossible);
        self::assertSame(['one'], $scalar->ids);
        self::assertSame([
            'name' => 'Alice',
        ], $scalar->remainingFields);

        $emptyList = FindByCriteria::from([
            'id'     => [],
            'status' => 'active',
        ]);

        self::assertTrue($emptyList->hasId);
        self::assertTrue($emptyList->multiple);
        self::assertFalse($emptyList->impossible);
        self::assertSame([], $emptyList->ids);
        self::assertSame([
            'status' => 'active',
        ], $emptyList->remainingFields);

        $list = FindByCriteria::from([
            'id' => ['two', 'one', 'two', 'three', 'one'],
            'ID' => 'preserved',
        ]);

        self::assertTrue($list->hasId);
        self::assertTrue($list->multiple);
        self::assertFalse($list->impossible);
        self::assertSame(['two', 'one', 'three'], $list->ids);
        self::assertSame([
            'ID' => 'preserved',
        ], $list->remainingFields);
    }

    public function test_it_marks_non_matching_scalar_types_as_impossible(): void
    {
        $resource = fopen('php://memory', 'rb');
        self::assertIsResource($resource);

        try {
            foreach (['', 1, 1.5, true, false, null, new \stdClass(), $resource] as $value) {
                $criteria = FindByCriteria::from([
                    'id'   => $value,
                    'name' => 'Alice',
                ]);

                self::assertTrue($criteria->hasId);
                self::assertFalse($criteria->multiple);
                self::assertTrue($criteria->impossible);
                self::assertSame([], $criteria->ids);
                self::assertSame([
                    'name' => 'Alice',
                ], $criteria->remainingFields);
            }
        } finally {
            fclose($resource);
        }
    }

    public function test_it_rejects_non_list_arrays(): void
    {
        try {
            FindByCriteria::from([
                'id' => [
                    'first' => 'one',
                ],
            ]);
            self::fail('Expected an invalid ID criterion exception.');
        } catch (InvalidIdCriterion $exception) {
            self::assertInstanceOf(InvalidArgumentException::class, $exception);
            self::assertSame('non-list', $exception->reason);
            self::assertNull($exception->index);
            self::assertSame('array', $exception->actualType);
            self::assertSame('The ID criterion array must be a list.', $exception->getMessage());
        }
    }

    public function test_it_rejects_invalid_list_entries(): void
    {
        $invalidValues = ['', 1, 1.5, true, false, null, new \stdClass()];
        $resource      = fopen('php://memory', 'rb');
        self::assertIsResource($resource);
        $invalidValues[] = $resource;

        try {
            foreach ($invalidValues as $value) {
                try {
                    FindByCriteria::from([
                        'id' => ['valid', $value],
                    ]);
                    self::fail('Expected an invalid ID criterion exception.');
                } catch (InvalidIdCriterion $exception) {
                    self::assertInstanceOf(InvalidArgumentException::class, $exception);
                    self::assertSame('invalid-entry', $exception->reason);
                    self::assertSame(1, $exception->index);
                    self::assertSame(get_debug_type($value), $exception->actualType);

                    if ('' === $value) {
                        self::assertSame(
                            'The ID criterion entry at index 1 must be a non-empty string; got string.',
                            $exception->getMessage()
                        );
                    }
                }
            }
        } finally {
            fclose($resource);
        }
    }
}
