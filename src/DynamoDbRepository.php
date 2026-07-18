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
        return $this->storage->find(RepositoryId::normalize($id));
    }

    /**
     * @param array<string, mixed> $fields
     */
    public function findBy(array $fields): array
    {
        if ([] === $fields) {
            return [];
        }

        $criteria = FindByCriteria::from($fields);

        if (!$criteria->hasId) {
            return $this->matching($this->findAll(), $fields);
        }

        if ($criteria->impossible || [] === $criteria->ids) {
            return [];
        }

        if ($criteria->multiple) {
            $models = $this->storage->findMany($criteria->ids);
        } else {
            $model  = $this->storage->find($criteria->ids[0]);
            $models = null === $model ? [] : [$model];
        }

        if ([] === $criteria->remainingFields) {
            return $models;
        }

        return $this->matching($models, $criteria->remainingFields);
    }

    /**
     * @param Identifiable[]      $models
     * @param array<string,mixed> $fields
     *
     * @return Identifiable[]
     */
    private function matching(array $models, array $fields): array
    {
        $items = array_filter(
            $models,
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
        $this->storage->remove(RepositoryId::normalize($id));
    }
}
