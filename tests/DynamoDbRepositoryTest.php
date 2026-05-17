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
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepository;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\DynamoDbTableManager;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use chrisjenkinson\DynamoDbReadModel\JsonDecoder;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;

final class DynamoDbRepositoryTest extends RepositoryTestCase
{
    private const TABLE_NAME = 'table';

    private const REPOSITORY_NAME = 'name';

    protected function createRepository(): Repository
    {
        $client = $this->createDynamoDbClient();

        $tableManager = new DynamoDbTableManager($client, new InputBuilder(), self::TABLE_NAME);
        $serializer   = new SimpleInterfaceSerializer();
        $factory      = new DynamoDbRepositoryFactory($client, $serializer, self::TABLE_NAME);

        $tableManager->deleteTable();
        $tableManager->createTable();

        return $factory->create(self::REPOSITORY_NAME, RepositoryTestReadModel::class);
    }

    /**
     * @test
     */
    public function it_removes_only_one_read_model_when_multiple_are_saved(): void
    {
        $model1 = $this->createReadModel('1', 'name1', 'foo1');
        $model2 = $this->createReadModel('2', 'name2', 'foo2');

        $this->repository->save($model1);
        $this->repository->save($model2);

        $this->repository->remove(1);

        $this->assertEquals([$model2], $this->repository->findAll());
    }

    /**
     * @test
     */
    public function it_finds_read_models_by_public_getter(): void
    {
        $model1 = $this->createReadModel('1', 'name1', 'foo1');
        $model2 = $this->createReadModel('2', 'name2', 'foo2');

        $this->repository->save($model1);
        $this->repository->save($model2);

        $this->assertEquals([$model1], $this->repository->findBy([
            'foo' => 'foo1',
        ]));
    }

    /**
     * @test
     */
    public function it_rejects_unexpected_serialized_classes_before_deserializing_them(): void
    {
        UnexpectedSerializableReadModel::$deserializeWasCalled = false;

        $this->createDynamoDbClient()->putItem([
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

    /**
     * @test
     */
    public function it_rejects_serialized_data_without_a_string_class(): void
    {
        $this->createDynamoDbClient()->putItem([
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

    /**
     * @test
     */
    public function it_rejects_deserialized_models_that_do_not_implement_identifiable(): void
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
            $client,
            new InputBuilder(),
            new SimpleInterfaceSerializer(),
            new JsonEncoder(),
            new JsonDecoder(),
            self::TABLE_NAME,
            'non-identifiable',
            NonIdentifiableSerializableReadModel::class
        );

        $this->expectException(UnexpectedReadModel::class);

        $repository->find('non-identifiable');
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
