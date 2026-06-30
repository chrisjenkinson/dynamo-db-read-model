<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Repository;

final class DynamoDbRepository implements Repository
{
    public function __construct(
        private readonly DynamoDbReadModelStorage $storage,
        private readonly ReadModelFieldMatcher $matcher
    ) {
    }

    public function save(Identifiable $data): void
    {
        $this->storage->save($this->storage->prepareSave($data));
    }

    public function find($id): ?Identifiable
    {
        return $this->storage->find((string) $id);
    }

    /**
     * @param array<string, mixed> $fields
     */
    public function findBy(array $fields): array
    {
        if ([] === $fields) {
            return [];
        }

        $items = array_filter(
            $this->findAll(),
            fn (Identifiable $model): bool => $this->matcher->matches($model, $fields)
        );

        return array_values($items);
    }

    public function findAll(): array
    {
        return $this->storage->findAll();
    }

    public function remove($id): void
    {
        $this->storage->remove((string) $id);
    }
}
