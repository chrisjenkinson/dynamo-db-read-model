<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\Core\Configuration;
use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DynamoDbReadModelStorage;
use chrisjenkinson\DynamoDbReadModel\DynamoDbTableManager;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use chrisjenkinson\DynamoDbReadModel\JsonDecoder;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use PHPUnit\Framework\TestCase;

final class DynamoDbReadModelStorageBatchGetIntegrationTest extends TestCase
{
    private const TABLE = 'batch-get-storage-test';

    private DynamoDbClient $client;

    private DynamoDbReadModelStorage $storage;

    protected function setUp(): void
    {
        $this->client = new DynamoDbClient(Configuration::create([
            'endpoint'        => $this->requiredEnvironmentVariable('DYNAMODB_ENDPOINT'),
            'accessKeyId'     => $this->requiredEnvironmentVariable('AWS_ACCESS_KEY_ID'),
            'accessKeySecret' => $this->requiredEnvironmentVariable('AWS_SECRET_ACCESS_KEY'),
        ]));
        $manager = new DynamoDbTableManager($this->client, new InputBuilder(), self::TABLE);
        $manager->deleteTable();
        $manager->createTable();
        $this->storage = $this->newStorage();
    }

    public function test_returns_requested_order_and_omits_missing_ids(): void
    {
        $this->saveModels(['one', 'two', 'three']);

        self::assertSame(['three', 'one'], $this->modelIds($this->storage->findMany(['three', 'missing', 'one'])));
    }

    public function test_reads_across_the_one_hundred_item_boundary(): void
    {
        $ids = array_map(static fn (int $number): string => 'id-' . $number, range(1, 101));
        $this->saveModels($ids);

        self::assertSame($ids, $this->modelIds($this->storage->findMany($ids)));
    }

    public function test_rejects_unexpected_serialized_class(): void
    {
        UnexpectedSerializableReadModel::$deserializeWasCalled = false;
        $this->putRaw('id', UnexpectedSerializableReadModel::class, 'id');

        try {
            $this->storage->findMany(['id']);
            self::fail('Expected an unexpected read model exception.');
        } catch (UnexpectedReadModel) {
            self::assertFalse(UnexpectedSerializableReadModel::$deserializeWasCalled);
        }
    }

    public function test_rejects_physical_and_payload_identifier_mismatch(): void
    {
        $this->putRaw('physical', RepositoryTestReadModel::class, 'payload');
        $this->expectException(UnexpectedReadModel::class);

        $this->storage->findMany(['physical']);
    }

    public function test_loaded_snapshot_suppresses_an_unchanged_save(): void
    {
        $model = new RepositoryTestReadModel('id', 'name', 'foo', []);
        $this->storage->save($this->storage->prepareSave($model));
        $loaded = $this->storage->findMany(['id'])[0];

        (new DynamoDbTableManager($this->client, new InputBuilder(), self::TABLE))->deleteTable();

        $this->storage->save($this->storage->prepareSave($loaded));
        self::assertSame('id', $loaded->getId());
    }

    /**
     * @param list<string> $ids
     */
    private function saveModels(array $ids): void
    {
        foreach ($ids as $id) {
            $model = new RepositoryTestReadModel($id, 'name', 'foo', []);
            $this->storage->save($this->storage->prepareSave($model));
        }
    }

    private function putRaw(string $physicalId, string $class, string $payloadId): void
    {
        $this->client->putItem([
            'TableName' => self::TABLE,
            'Item'      => [
                'Name' => new AttributeValue([
                    'S' => 'repository',
                ]),
                'Id' => new AttributeValue([
                    'S' => $physicalId,
                ]),
                'Data' => new AttributeValue([
                    'S' => json_encode([
                        'class'   => $class,
                        'payload' => [
                            'id'    => $payloadId,
                            'name'  => 'name',
                            'foo'   => 'foo',
                            'array' => [],
                        ],
                    ], JSON_THROW_ON_ERROR),
                ]),
            ],
        ])->resolve();
    }

    /**
     * @param Identifiable[] $models
     *
     * @return list<string>
     */
    private function modelIds(array $models): array
    {
        return array_map(static fn (Identifiable $model): string => $model->getId(), $models);
    }

    private function newStorage(): DynamoDbReadModelStorage
    {
        return new DynamoDbReadModelStorage(
            $this->client,
            new InputBuilder(),
            new SimpleInterfaceSerializer(),
            new JsonEncoder(),
            new JsonDecoder(),
            self::TABLE,
            'repository',
            RepositoryTestReadModel::class,
            new ReadModelSnapshotStore()
        );
    }

    private function requiredEnvironmentVariable(string $name): string
    {
        $value = getenv($name);

        if (false === $value || '' === $value) {
            throw new \RuntimeException(sprintf('Required environment variable "%s" is not set.', $name));
        }

        return $value;
    }
}
