<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

final class ReadModelSnapshotStore
{
    /**
     * @var array<string, ReadModelSnapshot>
     */
    private array $snapshots = [];

    public function hasSameSnapshot(ReadModelSnapshot $snapshot): bool
    {
        $remembered = $this->snapshots[$snapshot->key()] ?? null;

        return $remembered instanceof ReadModelSnapshot
            && $remembered->matches($snapshot->serializedData);
    }

    public function remember(ReadModelSnapshot $snapshot): void
    {
        $this->snapshots[$snapshot->key()] = $snapshot;
    }

    public function forget(string $table, string $name, string $class, string $id): void
    {
        unset($this->snapshots[ReadModelSnapshot::keyFor($table, $name, $class, $id)]);
    }

    public function clear(): void
    {
        $this->snapshots = [];
    }
}
