<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\Core\Configuration;
use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\DynamoDbTableManager;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use PHPUnit\Framework\TestCase;

final class DeferredDynamoDbRepositoryIntegrationTest extends TestCase
{
    private const TABLE_NAME = 'deferred-table';

    private const REPOSITORY_NAME = 'name';

    public function test_deferred_save_flush_can_be_read_by_an_immediate_repository(): void
    {
        $factory = $this->createFactory();

        $deferred = $factory->createDeferred(self::REPOSITORY_NAME, RepositoryTestReadModel::class);
        $model    = new RepositoryTestReadModel('id', 'name', 'foo', []);

        $deferred->save($model);
        self::assertNull($factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class)->find('id'));

        $deferred->flush();

        self::assertEquals($model, $factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class)->find('id'));
    }

    public function test_deferred_remove_flush_can_be_read_by_an_immediate_repository(): void
    {
        $factory = $this->createFactory();
        $model   = new RepositoryTestReadModel('id', 'name', 'foo', []);

        $factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class)->save($model);

        $deferred = $factory->createDeferred(self::REPOSITORY_NAME, RepositoryTestReadModel::class);
        $deferred->remove('id');
        self::assertEquals($model, $factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class)->find('id'));

        $deferred->flush();

        self::assertNull($factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class)->find('id'));
    }

    public function test_deferred_queries_merge_staged_saves_and_removes_against_dynamodb(): void
    {
        $factory = $this->createFactory();

        $persisted = new RepositoryTestReadModel('persisted', 'persisted', 'matched', []);
        $dirty     = new RepositoryTestReadModel('dirty', 'dirty', 'matched', []);

        $factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class)->save($persisted);

        $deferred = $factory->createDeferred(self::REPOSITORY_NAME, RepositoryTestReadModel::class);
        $deferred->remove('persisted');
        $deferred->save($dirty);

        self::assertEquals([$dirty], $deferred->findAll());
        self::assertEquals([$dirty], $deferred->findBy([
            'foo' => 'matched',
        ]));
    }

    public function test_deferred_id_list_lookup_merges_staged_and_persisted_models_in_requested_order_before_filtering(): void
    {
        $factory = $this->createFactory();

        $persistedMatching = new RepositoryTestReadModel('persisted-matching', 'persisted-matching', 'matched', []);
        $persistedOther    = new RepositoryTestReadModel('persisted-other', 'persisted-other', 'other', []);
        $dirty             = new RepositoryTestReadModel('dirty', 'dirty', 'matched', []);

        $immediate = $factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class);
        $immediate->save($persistedMatching);
        $immediate->save($persistedOther);

        $deferred = $factory->createDeferred(self::REPOSITORY_NAME, RepositoryTestReadModel::class);
        $deferred->save($dirty);

        self::assertEquals([$persistedMatching, $dirty], $deferred->findBy([
            'id'  => ['persisted-other', 'persisted-matching', 'dirty'],
            'foo' => 'matched',
        ]));
    }

    public function test_deferred_queries_reject_rows_whose_physical_id_does_not_match_the_payload_id(): void
    {
        $factory = $this->createFactory();
        $this->putSerializedReadModel('physical-id', new RepositoryTestReadModel('payload-id', 'name', 'foo', []));

        $this->expectException(UnexpectedReadModel::class);

        $factory->createDeferred(self::REPOSITORY_NAME, RepositoryTestReadModel::class)->findAll();
    }

    private function createFactory(): DynamoDbRepositoryFactory
    {
        $client = $this->createClient();

        $tableManager = new DynamoDbTableManager($client, new InputBuilder(), self::TABLE_NAME);
        $tableManager->deleteTable();
        $tableManager->createTable();

        return new DynamoDbRepositoryFactory($client, new SimpleInterfaceSerializer(), self::TABLE_NAME, new ReadModelSnapshotStore());
    }

    private function putSerializedReadModel(string $physicalId, RepositoryTestReadModel $model): void
    {
        $this->createClient()
            ->putItem([
                'TableName' => self::TABLE_NAME,
                'Item'      => [
                    'Name' => new AttributeValue([
                        'S' => self::REPOSITORY_NAME,
                    ]),
                    'Id' => new AttributeValue([
                        'S' => $physicalId,
                    ]),
                    'Data' => new AttributeValue([
                        'S' => (new JsonEncoder())->encode((new SimpleInterfaceSerializer())->serialize($model)),
                    ]),
                ],
            ])->resolve();
    }

    private function createClient(): DynamoDbClient
    {
        return new DynamoDbClient(Configuration::create([
            'endpoint'        => getenv('DYNAMODB_ENDPOINT') ?: 'http://dynamodb-local:8000',
            'accessKeyId'     => getenv('AWS_ACCESS_KEY_ID') ?: 'none',
            'accessKeySecret' => getenv('AWS_SECRET_ACCESS_KEY') ?: 'none',
        ]));
    }
}
