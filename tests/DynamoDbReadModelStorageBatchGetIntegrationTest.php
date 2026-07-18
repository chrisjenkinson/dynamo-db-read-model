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
            'endpoint'        => getenv('DYNAMODB_ENDPOINT') ?: 'http://127.0.0.1:8000',
            'accessKeyId'     => 'none',
            'accessKeySecret' => 'none',
        ]));
        $manager = new DynamoDbTableManager($this->client, new InputBuilder(), self::TABLE);
        $manager->deleteTable();
        $manager->createTable();
        $this->storage = $this->newStorage();
    }

    public function testReturnsRequestedOrderAndOmitsMissingIds(): void
    {
        $this->saveModels(['one', 'two', 'three']);

        self::assertSame(['three', 'one'], $this->modelIds($this->storage->findMany(['three', 'missing', 'one'])));
    }

    public function testReadsAcrossTheOneHundredItemBoundary(): void
    {
        $ids = array_map(static fn (int $number): string => 'id-' . $number, range(1, 101));
        $this->saveModels($ids);

        self::assertSame($ids, $this->modelIds($this->storage->findMany($ids)));
    }

    public function testRejectsUnexpectedSerializedClass(): void
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

    public function testRejectsPhysicalAndPayloadIdentifierMismatch(): void
    {
        $this->putRaw('physical', RepositoryTestReadModel::class, 'payload');
        $this->expectException(UnexpectedReadModel::class);

        $this->storage->findMany(['physical']);
    }

    public function testLoadedSnapshotSuppressesAnUnchangedSave(): void
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
}
