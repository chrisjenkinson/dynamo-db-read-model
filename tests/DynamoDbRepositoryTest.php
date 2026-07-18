<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\Core\Configuration;
use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use Broadway\ReadModel\Repository;
use Broadway\ReadModel\Testing\RepositoryTestCase;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DynamoDbReadModelStorage;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepository;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\DynamoDbTableManager;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use chrisjenkinson\DynamoDbReadModel\JsonDecoder;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelFieldMatcher;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;

final class DynamoDbRepositoryTest extends RepositoryTestCase
{
    private const TABLE_NAME = 'table';

    private const REPOSITORY_NAME = 'name';

    protected function createRepository(): Repository
    {
        $client = $this->createDynamoDbClient();

        $tableManager = new DynamoDbTableManager($client, new InputBuilder(), self::TABLE_NAME);
        $serializer   = new SimpleInterfaceSerializer();
        $factory      = new DynamoDbRepositoryFactory($client, $serializer, self::TABLE_NAME, new ReadModelSnapshotStore());

        $tableManager->deleteTable();
        $tableManager->createTable();

        return $factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class);
    }

    public function test_it_removes_only_one_read_model_when_multiple_are_saved(): void
    {
        $model1 = $this->createReadModel('1', 'name1', 'foo1');
        $model2 = $this->createReadModel('2', 'name2', 'foo2');

        $this->repository->save($model1);
        $this->repository->save($model2);

        $this->repository->remove(1);

        $this->assertEquals([$model2], $this->repository->findAll());
    }

    public function test_it_finds_read_models_by_public_getter(): void
    {
        $model1 = $this->createReadModel('1', 'name1', 'foo1');
        $model2 = $this->createReadModel('2', 'name2', 'foo2');

        $this->repository->save($model1);
        $this->repository->save($model2);

        $this->assertEquals([$model1], $this->repository->findBy([
            'foo' => 'foo1',
        ]));
    }

    public function test_it_finds_a_read_model_by_exact_lowercase_id(): void
    {
        $model = $this->createReadModel('one', 'name', 'foo');
        $this->repository->save($model);

        self::assertEquals([$model], $this->repository->findBy([
            'id' => 'one',
        ]));
    }

    public function test_it_finds_an_ordered_id_list_and_omits_missing_models(): void
    {
        $one   = $this->createReadModel('one', 'name one', 'foo');
        $two   = $this->createReadModel('two', 'name two', 'foo');
        $three = $this->createReadModel('three', 'name three', 'foo');
        $this->repository->save($one);
        $this->repository->save($two);
        $this->repository->save($three);

        self::assertEquals([$three, $one], $this->repository->findBy([
            'id' => ['three', 'missing', 'one'],
        ]));
    }

    public function test_it_applies_additional_predicates_to_an_id_list(): void
    {
        $matching = $this->createReadModel('one', 'name one', 'matching');
        $other    = $this->createReadModel('two', 'name two', 'other');
        $outside  = $this->createReadModel('outside', 'name outside', 'matching');
        $this->repository->save($matching);
        $this->repository->save($other);
        $this->repository->save($outside);

        self::assertEquals([$matching], $this->repository->findBy([
            'id'  => ['one', 'two'],
            'foo' => 'matching',
        ]));
    }

    public function test_it_rejects_unexpected_serialized_classes_before_deserializing_them(): void
    {
        UnexpectedSerializableReadModel::$deserializeWasCalled = false;

        $this->createDynamoDbClient()
            ->putItem([
                'TableName' => self::TABLE_NAME,
                'Item'      => [
                    'Name' => new AttributeValue([
                        'S' => self::REPOSITORY_NAME,
                    ]),
                    'Id' => new AttributeValue([
                        'S' => 'unexpected',
                    ]),
                    'Data' => new AttributeValue([
                        'S' => json_encode([
                            'class'   => UnexpectedSerializableReadModel::class,
                            'payload' => [
                                'id' => 'unexpected',
                            ],
                        ], JSON_THROW_ON_ERROR),
                    ]),
                ],
            ])->resolve();

        $this->expectException(UnexpectedReadModel::class);

        try {
            $this->repository->find('unexpected');
        } finally {
            $this->assertFalse(UnexpectedSerializableReadModel::$deserializeWasCalled);
        }
    }

    public function test_it_rejects_serialized_data_without_a_string_class(): void
    {
        $this->createDynamoDbClient()
            ->putItem([
                'TableName' => self::TABLE_NAME,
                'Item'      => [
                    'Name' => new AttributeValue([
                        'S' => self::REPOSITORY_NAME,
                    ]),
                    'Id' => new AttributeValue([
                        'S' => 'invalid-class',
                    ]),
                    'Data' => new AttributeValue([
                        'S' => json_encode([
                            'class'   => true,
                            'payload' => [
                                'id' => 'invalid-class',
                            ],
                        ], JSON_THROW_ON_ERROR),
                    ]),
                ],
            ])->resolve();

        $this->expectException(UnexpectedReadModel::class);

        $this->repository->find('invalid-class');
    }

    public function test_it_rejects_deserialized_models_that_do_not_implement_identifiable(): void
    {
        $client = $this->createDynamoDbClient();
        $client->putItem([
            'TableName' => self::TABLE_NAME,
            'Item'      => [
                'Name' => new AttributeValue([
                    'S' => 'non-identifiable',
                ]),
                'Id' => new AttributeValue([
                    'S' => 'non-identifiable',
                ]),
                'Data' => new AttributeValue([
                    'S' => json_encode([
                        'class'   => NonIdentifiableSerializableReadModel::class,
                        'payload' => [
                            'id' => 'non-identifiable',
                        ],
                    ], JSON_THROW_ON_ERROR),
                ]),
            ],
        ])->resolve();

        $repository = new DynamoDbRepository(
            new DynamoDbReadModelStorage(
                $client,
                new InputBuilder(),
                new SimpleInterfaceSerializer(),
                new JsonEncoder(),
                new JsonDecoder(),
                self::TABLE_NAME,
                'non-identifiable',
                NonIdentifiableSerializableReadModel::class,
                new ReadModelSnapshotStore()
            ),
            new ReadModelFieldMatcher()
        );

        $this->expectException(UnexpectedReadModel::class);

        $repository->find('non-identifiable');
    }

    public function test_it_rejects_rows_whose_physical_id_does_not_match_the_payload_id_when_finding_one_model(): void
    {
        $this->putSerializedReadModel('physical-id', new RepositoryTestReadModel('payload-id', 'name', 'foo', []));

        $this->expectException(UnexpectedReadModel::class);

        $this->repository->find('physical-id');
    }

    public function test_it_rejects_rows_whose_physical_id_does_not_match_the_payload_id_when_querying_models(): void
    {
        $this->putSerializedReadModel('physical-id', new RepositoryTestReadModel('payload-id', 'name', 'foo', []));

        $this->expectException(UnexpectedReadModel::class);

        $this->repository->findBy([
            'foo' => 'foo',
        ]);
    }

    private function putSerializedReadModel(string $physicalId, RepositoryTestReadModel $model): void
    {
        $this->createDynamoDbClient()
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

    private function createDynamoDbClient(): DynamoDbClient
    {
        return new DynamoDbClient(Configuration::create([
            'endpoint'        => getenv('DYNAMODB_ENDPOINT') ?: 'http://dynamodb-local:8000',
            'accessKeyId'     => getenv('AWS_ACCESS_KEY_ID') ?: 'none',
            'accessKeySecret' => getenv('AWS_SECRET_ACCESS_KEY') ?: 'none',
        ]));
    }
}
