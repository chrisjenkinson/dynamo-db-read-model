<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use Broadway\ReadModel\Identifiable;

final class DeferredDynamoDbRepository implements FlushableRepository
{
    /**
     * @var array<string, Identifiable>
     */
    private array $managed = [];

    /**
     * @var array<string, PreparedReadModelSave>
     */
    private array $dirty = [];

    /**
     * @var string[]
     */
    private array $removed = [];

    public function __construct(
        private readonly DynamoDbReadModelStorage $storage,
        private readonly ReadModelFieldMatcher $matcher
    ) {
    }

    public function save(Identifiable $data): void
    {
        $id = $data->getId();

        $this->dirty[$id] = $this->storage->prepareSave($data);
        unset($this->managed[$id]);

        $this->removed = array_diff($this->removed, [$id]);
    }

    public function find($id): ?Identifiable
    {
        $id = (string) $id;

        if (in_array($id, $this->removed, true)) {
            return null;
        }

        if (isset($this->managed[$id])) {
            return $this->managed[$id];
        }

        if (isset($this->dirty[$id])) {
            $this->managed[$id] = $this->storage->modelFromPreparedSave($this->dirty[$id]);

            return $this->managed[$id];
        }

        $model = $this->storage->find($id);

        if ($model instanceof Identifiable) {
            $this->managed[$id] = $model;
        }

        return $model;
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
            $models = $this->findMany($criteria->ids);
        } else {
            $model  = $this->find($criteria->ids[0]);
            $models = null === $model ? [] : [$model];
        }

        if ([] === $criteria->remainingFields) {
            return $models;
        }

        return $this->matching($models, $criteria->remainingFields);
    }

    /**
     * @param list<string> $ids
     *
     * @return Identifiable[]
     */
    private function findMany(array $ids): array
    {
        $removed    = array_fill_keys($this->removed, true);
        $unresolved = [];

        foreach ($ids as $id) {
            if (isset($removed[$id])) {
                continue;
            }

            if (isset($this->managed[$id])) {
                continue;
            }

            if (isset($this->dirty[$id])) {
                $this->managed[$id] = $this->storage->modelFromPreparedSave($this->dirty[$id]);

                continue;
            }

            $unresolved[] = $id;
        }

        foreach ($this->storage->findMany($unresolved) as $model) {
            $this->managed[$model->getId()] = $model;
        }

        $models = [];

        foreach ($ids as $id) {
            if (isset($this->managed[$id]) && !isset($removed[$id])) {
                $models[] = $this->managed[$id];
            }
        }

        return $models;
    }

    /**
     * @param Identifiable[]      $models
     * @param array<string,mixed> $fields
     *
     * @return Identifiable[]
     */
    private function matching(array $models, array $fields): array
    {
        return array_values(array_filter(
            $models,
            fn (Identifiable $model): bool => $this->matcher->matches($model, $fields)
        ));
    }

    public function findAll(): array
    {
        $models        = [];
        $managedBefore = array_fill_keys(array_keys($this->managed), true);

        foreach ($this->storage->findAll() as $model) {
            $id = $model->getId();

            if (in_array($id, $this->removed, true)) {
                continue;
            }

            $this->managed[$id] = $this->managed[$id] ?? $model;
            $models[$id]        = $this->managed[$id];
        }

        foreach ($this->dirty as $id => $save) {
            if (!in_array($id, $this->removed, true)) {
                if (!isset($managedBefore[$id])) {
                    $this->managed[$id] = $this->storage->modelFromPreparedSave($save);
                }

                $models[$id] = $this->managed[$id];
            }
        }

        return array_values($models);
    }

    public function remove($id): void
    {
        $id = (string) $id;

        unset($this->managed[$id], $this->dirty[$id]);

        if (!in_array($id, $this->removed, true)) {
            $this->removed[] = $id;
        }
    }

    public function flush(): void
    {
        $this->flushWithContext([]);
    }

    /**
     * @param array<string, scalar|null> $context
     */
    public function flushWithContext(array $context): void
    {
        foreach ($this->removed as $index => $id) {
            try {
                $this->storage->remove($id);
            } catch (\Throwable $exception) {
                throw new DeferredFlushFailed(DeferredOperation::REMOVE, $id, $this->storage->table(), $this->storage->name(), $this->storage->readModelClass(), $context, $exception);
            }

            unset($this->removed[$index]);
        }

        foreach ($this->dirty as $id => $save) {
            try {
                $this->storage->save($save);
            } catch (\Throwable $exception) {
                throw new DeferredFlushFailed(DeferredOperation::SAVE, $id, $this->storage->table(), $this->storage->name(), $this->storage->readModelClass(), $context, $exception);
            }

            unset($this->dirty[$id]);
        }
    }

    public function clear(): void
    {
        $this->managed = [];
        $this->dirty   = [];
        $this->removed = [];
    }

    public function pendingOperations(): array
    {
        $operations = [];

        foreach ($this->removed as $id) {
            $operations[] = new DeferredOperation(DeferredOperation::REMOVE, $id);
        }

        foreach ($this->dirty as $id => $save) {
            $operations[] = new DeferredOperation(DeferredOperation::SAVE, $id, $this->storage->modelFromPreparedSave($save));
        }

        return $operations;
    }
}
