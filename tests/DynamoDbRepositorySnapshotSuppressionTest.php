<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\Core\Response;
use AsyncAws\Core\Test\Http\SimpleMockedResponse;
use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\Result\DeleteItemOutput;
use AsyncAws\DynamoDb\Result\GetItemOutput;
use AsyncAws\DynamoDb\Result\PutItemOutput;
use AsyncAws\DynamoDb\Result\QueryOutput;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshot;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\MockHttpClient;

final class DynamoDbRepositorySnapshotSuppressionTest extends TestCase
{
    public function test_it_skips_the_physical_write_when_the_saved_model_matches_the_snapshot_loaded_by_find(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $model = $repository->find('id');
        self::assertInstanceOf(RepositoryTestReadModel::class, $model);

        $repository->save($model);
    }

    public function test_it_writes_when_the_saved_model_differs_from_the_snapshot_loaded_by_find(): void
    {
        $putItemOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $model = $repository->find('id');
        self::assertInstanceOf(RepositoryTestReadModel::class, $model);

        $repository->save(new RepositoryTestReadModel('id', 'changed', 'foo', []));
    }

    public function test_it_writes_when_no_prior_snapshot_is_available(): void
    {
        $putItemOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('getItem');
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));
    }

    public function test_it_rejects_saving_a_read_model_for_a_different_repository_class(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $this->expectException(UnexpectedReadModel::class);

        $repository->save(new UnexpectedSerializableReadModel('wrong-class'));
    }

    public function test_it_skips_the_second_physical_write_when_the_same_state_was_already_saved(): void
    {
        $putItemOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);
        $model      = new RepositoryTestReadModel('id', 'name', 'foo', []);

        $repository->save($model);
        $repository->save($model);
    }

    public function test_it_does_not_snapshot_failed_saves(): void
    {
        $successfulOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->exactly(2))
            ->method('putItem')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new \RuntimeException('failed')),
                $successfulOutput
            );

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);
        $model      = new RepositoryTestReadModel('id', 'name', 'foo', []);

        try {
            $repository->save($model);
            self::fail('Expected the first save to fail.');
        } catch (\Throwable) {
        }

        $repository->save($model);
    }

    public function test_it_clears_the_snapshot_when_a_model_is_removed(): void
    {
        $deleteItemOutput = new DeleteItemOutput($this->createResponse());
        $putItemOutput    = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->once())->method('deleteItem')->willReturn($deleteItemOutput);
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);
        $model      = $repository->find('id');
        self::assertInstanceOf(RepositoryTestReadModel::class, $model);

        $repository->remove('id');
        $repository->save($model);
    }

    public function test_it_rejects_find_rows_whose_physical_id_does_not_match_the_payload_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('payload-id', 'name', 'foo', [])));
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $this->expectException(UnexpectedReadModel::class);

        $repository->find('physical-id');
    }

    public function test_it_shares_snapshots_across_repositories_created_by_the_same_factory(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->never())->method('putItem');

        $factory = $this->createFactory($client);

        $model = $factory->create('items', RepositoryTestReadModel::class)->find('id');
        self::assertInstanceOf(RepositoryTestReadModel::class, $model);

        $factory->create('items', RepositoryTestReadModel::class)->save($model);
    }

    public function test_it_does_not_share_snapshots_between_repository_names(): void
    {
        $putItemOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $factory = $this->createFactory($client);

        $model = $factory->create('items', RepositoryTestReadModel::class)->find('id');
        self::assertInstanceOf(RepositoryTestReadModel::class, $model);

        $factory->create('other_items', RepositoryTestReadModel::class)->save($model);
    }

    public function test_it_does_not_share_snapshots_between_classes(): void
    {
        $store = new ReadModelSnapshotStore();
        $store->remember(new ReadModelSnapshot('table', 'items', RepositoryTestReadModel::class, 'id', [
            'payload' => [
                'id' => 'id',
            ],
        ]));

        self::assertFalse($store->hasSameSnapshot(new ReadModelSnapshot('table', 'items', UnexpectedSerializableReadModel::class, 'id', [
            'payload' => [
                'id' => 'id',
            ],
        ])));
    }

    public function test_it_writes_after_factory_snapshots_are_cleared(): void
    {
        $putItemOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $factory    = $this->createFactory($client);
        $repository = $factory->create('items', RepositoryTestReadModel::class);

        $model = $repository->find('id');
        self::assertInstanceOf(RepositoryTestReadModel::class, $model);

        $factory->clearSnapshots();
        $repository->save($model);
    }

    public function test_it_snapshots_models_loaded_by_find_all(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('query')
            ->willReturn($this->queryOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $models = $repository->findAll();
        self::assertCount(1, $models);
        self::assertContainsOnlyInstancesOf(RepositoryTestReadModel::class, $models);

        $repository->save($models[0]);
    }

    public function test_it_snapshots_models_loaded_by_find_by(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('query')
            ->willReturn($this->queryOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $models = $repository->findBy([
            'foo' => 'foo',
        ]);
        self::assertCount(1, $models);
        self::assertContainsOnlyInstancesOf(RepositoryTestReadModel::class, $models);

        $repository->save($models[0]);
    }

    public function test_it_snapshots_models_read_by_find_by_even_when_they_are_filtered_out(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('query')
            ->willReturn($this->queryOutputFor(new RepositoryTestReadModel('id', 'name', 'not-matching', [])));
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        self::assertSame([], $repository->findBy([
            'foo' => 'matching',
        ]));
        $repository->save(new RepositoryTestReadModel('id', 'name', 'not-matching', []));
    }

    public function test_it_rejects_query_rows_whose_physical_id_does_not_match_the_payload_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('query')
            ->willReturn($this->queryOutputFor(new RepositoryTestReadModel('payload-id', 'name', 'foo', []), 'physical-id'));
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $this->expectException(UnexpectedReadModel::class);

        $repository->findAll();
    }

    public function test_it_rejects_find_by_rows_whose_physical_id_does_not_match_the_payload_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('query')
            ->willReturn($this->queryOutputFor(new RepositoryTestReadModel('payload-id', 'name', 'foo', []), 'physical-id'));
        $client->expects($this->never())->method('putItem');

        $repository = $this->createFactory($client)->create('items', RepositoryTestReadModel::class);

        $this->expectException(UnexpectedReadModel::class);

        $repository->findBy([
            'foo' => 'foo',
        ]);
    }

    private function createFactory(DynamoDbClient $client): DynamoDbRepositoryFactory
    {
        return new DynamoDbRepositoryFactory($client, new SimpleInterfaceSerializer(), 'table', new ReadModelSnapshotStore());
    }

    private function getItemOutputFor(RepositoryTestReadModel $model): GetItemOutput
    {
        $output = $this->createStub(GetItemOutput::class);
        $output->method('getItem')->willReturn([
            'Data' => new AttributeValue([
                'S' => (new JsonEncoder())->encode((new SimpleInterfaceSerializer())->serialize($model)),
            ]),
        ]);

        return $output;
    }

    private function queryOutputFor(RepositoryTestReadModel $model, ?string $physicalId = null): QueryOutput
    {
        $physicalId ??= $model->getId();

        $output = $this->createStub(QueryOutput::class);
        $output->method('getCount')->willReturn(1);
        $output->method('getItems')->willReturn([
            [
                'Id' => new AttributeValue([
                    'S' => $physicalId,
                ]),
                'Data' => new AttributeValue([
                    'S' => (new JsonEncoder())->encode((new SimpleInterfaceSerializer())->serialize($model)),
                ]),
            ],
        ]);

        return $output;
    }

    private function createResponse(): Response
    {
        $client = new MockHttpClient(new SimpleMockedResponse('{}'));

        return new Response($client->request('POST', 'http://localhost'), $client, new NullLogger());
    }
}
