<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\ValueObject\KeysAndAttributes;
use Broadway\ReadModel\Identifiable;
use Broadway\Serializer\Serializer;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedEncodedData;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use Closure;

final class DynamoDbReadModelStorage
{
    private const BATCH_SIZE = 100;

    private const BATCH_ATTEMPTS = 4;

    private const RETRY_DELAY_CAPS = [25000, 50000, 100000];

    /**
     * @var Closure(int): void
     */
    private readonly Closure $delay;

    /**
     * @param null|Closure(int): void $delay
     */
    public function __construct(
        private readonly DynamoDbClient $client,
        private readonly InputBuilder $inputBuilder,
        private readonly Serializer $serializer,
        private readonly JsonEncoder $jsonEncoder,
        private readonly JsonDecoder $jsonDecoder,
        private readonly string $table,
        private readonly string $name,
        private readonly string $class,
        private readonly ReadModelSnapshotStore $snapshots,
        ?Closure $delay = null
    ) {
        $this->delay = $delay ?? static function (int $max): void {
            usleep(random_int(0, $max));
        };
    }

    public function table(): string
    {
        return $this->table;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function readModelClass(): string
    {
        return $this->class;
    }

    public function prepareSave(Identifiable $data): PreparedReadModelSave
    {
        if (!($data instanceof $this->class)) {
            throw new UnexpectedReadModel(actual: $data::class, expected: $this->class);
        }

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

        $this->client->putItem($putItemInput)
            ->resolve();
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
     * @param list<string> $ids
     *
     * @return Identifiable[]
     */
    public function findMany(array $ids): array
    {
        $ids = array_values(array_unique($ids));

        if ([] === $ids) {
            return [];
        }

        $modelsById = [];

        foreach (array_chunk($ids, self::BATCH_SIZE) as $chunk) {
            $input          = $this->inputBuilder->buildBatchGetItemInput($this->table, $this->name, $chunk);
            $outstandingIds = $chunk;

            for ($attempt = 1; $attempt <= self::BATCH_ATTEMPTS; ++$attempt) {
                $result        = $this->client->batchGetItem($input);
                $responseItems = $this->validateResponses($result->getResponses(), $outstandingIds);

                $responseIds = array_map(static fn (array $item): string => $item['Id']->getS() ?? '', $responseItems);

                $unprocessedKeys   = $result->getUnprocessedKeys();
                $keysAndAttributes = null;
                $unresolvedIds     = [];

                if ([] !== $unprocessedKeys) {
                    [$keysAndAttributes, $unresolvedIds] = $this->validateUnprocessedKeys($unprocessedKeys, $outstandingIds);
                }

                if ([] !== array_intersect($responseIds, $unresolvedIds)) {
                    throw new UnexpectedEncodedData('BatchGetItem', 'disjoint response and unprocessed ids', [$responseIds, $unresolvedIds]);
                }

                foreach ($responseItems as $item) {
                    $model                       = $this->modelFromBatchItem($item);
                    $modelsById[$model->getId()] = $model;
                }

                if ([] === $unprocessedKeys) {
                    break;
                }

                if (self::BATCH_ATTEMPTS === $attempt) {
                    throw new BatchGetRetriesExhausted(
                        $this->table,
                        $this->name,
                        $this->class,
                        array_values(array_intersect($chunk, $unresolvedIds)),
                        $attempt
                    );
                }

                ($this->delay)(self::RETRY_DELAY_CAPS[$attempt - 1]);
                $input          = $this->inputBuilder->buildBatchGetItemRetryInput($this->table, $keysAndAttributes);
                $outstandingIds = $unresolvedIds;
            }
        }

        $models = [];

        foreach ($ids as $id) {
            if (isset($modelsById[$id])) {
                $models[] = $modelsById[$id];
            }
        }

        return $models;
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
        $this->client->deleteItem($this->inputBuilder->buildDeleteItemInput($this->table, $this->name, $id))
            ->resolve();
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
     * @param array<string, \AsyncAws\DynamoDb\ValueObject\AttributeValue> $item
     */
    private function modelFromBatchItem(array $item): Identifiable
    {
        $observedId = ($item['Id'] ?? null)?->getS();

        if (!is_string($observedId)) {
            throw new UnexpectedEncodedData('Id', 'string', $observedId);
        }

        $encodedData = ($item['Data'] ?? null)?->getS();

        if (!is_string($encodedData)) {
            throw new UnexpectedEncodedData('Data', 'string', $encodedData);
        }

        return $this->deserializeReadModel($encodedData, $observedId);
    }

    /**
     * @param array<string, array<array<string, \AsyncAws\DynamoDb\ValueObject\AttributeValue>>> $responses
     * @param list<string>                                                                         $outstandingIds
     *
     * @return array<array<string, \AsyncAws\DynamoDb\ValueObject\AttributeValue>>
     */
    private function validateResponses(array $responses, array $outstandingIds): array
    {
        if ([] !== $responses && [$this->table] !== array_keys($responses)) {
            throw new UnexpectedEncodedData('Responses', sprintf('only table %s', $this->table), $responses);
        }

        $items          = $responses[$this->table] ?? [];
        $outstandingSet = array_fill_keys($outstandingIds, true);
        $observedIds    = [];

        foreach ($items as $item) {
            $id = ($item['Id'] ?? null)?->getS();

            if (!is_string($id)) {
                throw new UnexpectedEncodedData('Id', 'string', $id);
            }

            if ('' === $id || !isset($outstandingSet[$id]) || isset($observedIds[$id])) {
                throw new UnexpectedEncodedData('Id', 'a unique non-empty id outstanding in the current attempt', $id);
            }

            $encodedData = ($item['Data'] ?? null)?->getS();

            if (!is_string($encodedData)) {
                throw new UnexpectedEncodedData('Data', 'string', $encodedData);
            }

            $observedIds[$id] = true;
        }

        return $items;
    }

    /**
     * @param array<string, KeysAndAttributes> $unprocessedKeys
     * @param list<string>                     $outstandingIds
     *
     * @return array{KeysAndAttributes, list<string>}
     */
    private function validateUnprocessedKeys(array $unprocessedKeys, array $outstandingIds): array
    {
        if ([$this->table] !== array_keys($unprocessedKeys)) {
            throw new UnexpectedEncodedData('UnprocessedKeys', sprintf('only table %s', $this->table), $unprocessedKeys);
        }

        $keysAndAttributes = $unprocessedKeys[$this->table];
        $keys              = $keysAndAttributes->getKeys();

        if ([] === $keys) {
            throw new UnexpectedEncodedData('UnprocessedKeys', 'at least one key', $keys);
        }

        $outstandingSet = array_fill_keys($outstandingIds, true);
        $observedIds    = [];
        $unresolvedIds  = [];

        foreach ($keys as $key) {
            $name = ($key['Name'] ?? null)?->getS();

            if (!is_string($name)) {
                throw new UnexpectedEncodedData('UnprocessedKeys.Name', 'string', $name);
            }

            if ($this->name !== $name) {
                throw new UnexpectedEncodedData('UnprocessedKeys.Name', $this->name, $name);
            }

            $id = ($key['Id'] ?? null)?->getS();

            if (!is_string($id)) {
                throw new UnexpectedEncodedData('UnprocessedKeys.Id', 'string', $id);
            }

            if ('' === $id || !isset($outstandingSet[$id]) || isset($observedIds[$id])) {
                throw new UnexpectedEncodedData('UnprocessedKeys.Id', 'a unique non-empty id outstanding in the current attempt', $id);
            }

            $observedIds[$id] = true;
            $unresolvedIds[]  = $id;
        }

        return [$keysAndAttributes, $unresolvedIds];
    }

    /**
     * @param array<mixed> $serializedData
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
            throw new UnexpectedReadModel(actual: get_debug_type($data), expected: $this->class);
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
