<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use AsyncAws\DynamoDb\DynamoDbClient;
use Broadway\ReadModel\Identifiable;
use Broadway\Serializer\Serializer;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedEncodedData;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;

final class DynamoDbReadModelStorage
{
    public function __construct(
        private readonly DynamoDbClient $client,
        private readonly InputBuilder $inputBuilder,
        private readonly Serializer $serializer,
        private readonly JsonEncoder $jsonEncoder,
        private readonly JsonDecoder $jsonDecoder,
        private readonly string $table,
        private readonly string $name,
        private readonly string $class,
        private readonly ReadModelSnapshotStore $snapshots
    ) {
    }

    public function prepareSave(Identifiable $data): PreparedReadModelSave
    {
        return new PreparedReadModelSave($data->getId(), $this->serializer->serialize($data));
    }

    public function save(PreparedReadModelSave $save): void
    {
        $snapshot = new ReadModelSnapshot($this->table, $this->name, $this->class, $save->id, $save->serializedData);

        if ($this->snapshots->hasSameSnapshot($snapshot)) {
            return;
        }

        $encodedData  = $this->jsonEncoder->encode($save->serializedData);
        $putItemInput = $this->inputBuilder->buildPutItemInput($this->table, $this->name, $save->id, $encodedData);

        $this->client->putItem($putItemInput)->resolve();
        $this->snapshots->remember($snapshot);
    }

    public function find(string $id): ?Identifiable
    {
        $result = $this->client->getItem($this->inputBuilder->buildGetItemInput($this->table, $this->name, $id));

        $item = $result->getItem();

        if ([] === $item) {
            return null;
        }

        $encodedData = $item['Data']->getS();

        if (!is_string($encodedData)) {
            throw new UnexpectedEncodedData('Data', 'string', $encodedData);
        }

        return $this->deserializeReadModel($encodedData, $id);
    }

    /**
     * @return Identifiable[]
     */
    public function findAll(): array
    {
        $result = $this->client->query($this->inputBuilder->buildQueryInput($this->table, $this->name));

        if (0 === $result->getCount()) {
            return [];
        }

        $models = [];

        foreach ($result->getItems() as $item) {
            $encodedData = $item['Data']->getS();

            if (!is_string($encodedData)) {
                throw new UnexpectedEncodedData('Data', 'string', $encodedData);
            }

            $observedId = $item['Id']->getS();

            if (!is_string($observedId)) {
                throw new UnexpectedEncodedData('Id', 'string', $observedId);
            }

            $models[] = $this->deserializeReadModel($encodedData, $observedId);
        }

        return $models;
    }

    public function remove(string $id): void
    {
        $this->client->deleteItem($this->inputBuilder->buildDeleteItemInput($this->table, $this->name, $id))->resolve();
        $this->snapshots->forget($this->table, $this->name, $this->class, $id);
    }

    public function modelFromPreparedSave(PreparedReadModelSave $save): Identifiable
    {
        return $this->deserializeSerializedReadModel($save->serializedData, $save->id, false);
    }

    private function deserializeReadModel(string $encodedData, string $observedId): Identifiable
    {
        return $this->deserializeSerializedReadModel($this->jsonDecoder->decode($encodedData), $observedId, true);
    }

    /**
     * @param array<string, mixed> $serializedData
     */
    private function deserializeSerializedReadModel(array $serializedData, string $observedId, bool $rememberSnapshot): Identifiable
    {
        if (!isset($serializedData['class']) || !is_string($serializedData['class'])) {
            throw new UnexpectedReadModel(actual: get_debug_type($serializedData['class'] ?? null), expected: $this->class);
        }

        if ($serializedData['class'] !== $this->class) {
            throw new UnexpectedReadModel(actual: $serializedData['class'], expected: $this->class);
        }

        $data = $this->serializer->deserialize($serializedData);

        if (!($data instanceof $this->class && $data instanceof Identifiable)) {
            throw new UnexpectedReadModel(actual: $data::class, expected: $this->class);
        }

        $id = $data->getId();

        if ($id !== $observedId) {
            throw new UnexpectedReadModel(actual: sprintf('%s with id %s', $data::class, $id), expected: sprintf('%s with id %s', $this->class, $observedId));
        }

        if ($rememberSnapshot) {
            $this->snapshots->remember(new ReadModelSnapshot($this->table, $this->name, $this->class, $id, $serializedData));
        }

        return $data;
    }
}
