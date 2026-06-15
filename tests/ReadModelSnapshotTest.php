<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshot;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use PHPUnit\Framework\TestCase;

final class ReadModelSnapshotTest extends TestCase
{
    public function test_key_matches_key_for(): void
    {
        $snapshot = new ReadModelSnapshot('table', 'items', RepositoryTestReadModel::class, 'id', [
            'payload' => [
                'id' => 'id',
            ],
        ]);

        self::assertSame(
            ReadModelSnapshot::keyFor('table', 'items', RepositoryTestReadModel::class, 'id'),
            $snapshot->key()
        );
    }

    public function test_key_encoding_is_unambiguous_for_delimiter_like_ids(): void
    {
        self::assertNotSame(
            ReadModelSnapshot::keyFor('table', 'items', RepositoryTestReadModel::class, "a\0b"),
            ReadModelSnapshot::keyFor("table\0items", RepositoryTestReadModel::class, 'a', 'b')
        );
    }

    public function test_matches_exact_serialized_state(): void
    {
        $snapshot = new ReadModelSnapshot('table', 'items', RepositoryTestReadModel::class, 'id', [
            'payload' => [
                'id'   => 'id',
                'name' => 'name',
            ],
        ]);

        self::assertTrue($snapshot->matches([
            'payload' => [
                'id'   => 'id',
                'name' => 'name',
            ],
        ]));
        self::assertFalse($snapshot->matches([
            'payload' => [
                'name' => 'name',
                'id'   => 'id',
            ],
        ]));
    }

    public function test_store_isolates_snapshots_by_table_repository_name_and_class(): void
    {
        $store = new ReadModelSnapshotStore();
        $store->remember(new ReadModelSnapshot('table', 'items', RepositoryTestReadModel::class, 'id', [
            'payload' => [
                'id' => 'id',
            ],
        ]));

        self::assertFalse($store->hasSameSnapshot(new ReadModelSnapshot('other-table', 'items', RepositoryTestReadModel::class, 'id', [
            'payload' => [
                'id' => 'id',
            ],
        ])));
        self::assertFalse($store->hasSameSnapshot(new ReadModelSnapshot('table', 'other-items', RepositoryTestReadModel::class, 'id', [
            'payload' => [
                'id' => 'id',
            ],
        ])));
        self::assertFalse($store->hasSameSnapshot(new ReadModelSnapshot('table', 'items', UnexpectedSerializableReadModel::class, 'id', [
            'payload' => [
                'id' => 'id',
            ],
        ])));
    }
}
