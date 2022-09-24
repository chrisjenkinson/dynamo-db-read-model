<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\Core\Configuration;
use AsyncAws\DynamoDb\DynamoDbClient;
use Broadway\ReadModel\Repository;
use Broadway\ReadModel\Testing\RepositoryTestCase;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\DynamoDbTableManager;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;

final class DynamoDbRepositoryTest extends RepositoryTestCase
{
    protected function createRepository(): Repository
    {
        $client = new DynamoDbClient(Configuration::create([
            'endpoint'        => 'http://dynamodb-local:8000',
            'accessKeyId'     => '',
            'accessKeySecret' => '',
        ]));
        $tableName = 'table';

        $tableManager = new DynamoDbTableManager($client, new InputBuilder(), $tableName);
        $serializer   = new SimpleInterfaceSerializer();
        $factory      = new DynamoDbRepositoryFactory($client, $serializer, $tableName);

        $tableManager->deleteTable();
        $tableManager->createTable();

        return $factory->create('name', RepositoryTestReadModel::class);
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
}
