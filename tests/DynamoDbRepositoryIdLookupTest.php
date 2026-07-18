<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\Result\BatchGetItemOutput;
use AsyncAws\DynamoDb\Result\GetItemOutput;
use AsyncAws\DynamoDb\Result\QueryOutput;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepository;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\Exception\InvalidIdCriterion;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use PHPUnit\Framework\TestCase;

final class DynamoDbRepositoryIdLookupTest extends TestCase
{
    public function test_scalar_lowercase_id_uses_get_item_without_querying_or_batching(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('getItem')->willReturn($this->getOutput($this->model('one')));
        $client->expects(self::never())->method('query');
        $client->expects(self::never())->method('batchGetItem');

        self::assertSame(['one'], $this->ids($this->repository($client)->findBy([
            'id' => 'one',
        ])));
    }

    public function test_missing_scalar_lowercase_id_returns_an_empty_list(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('getItem')->willReturn($this->getOutput());
        $client->expects(self::never())->method('query');
        $client->expects(self::never())->method('batchGetItem');

        self::assertSame([], $this->repository($client)->findBy([
            'id' => 'missing',
        ]));
    }

    public function test_scalar_lowercase_id_applies_only_the_additional_predicates(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(2))->method('getItem')->willReturn($this->getOutput($this->model('one', 'foo')));
        $client->expects(self::never())->method('query');
        $client->expects(self::never())->method('batchGetItem');
        $repository = $this->repository($client);

        self::assertSame(['one'], $this->ids($repository->findBy([
            'id'  => 'one',
            'foo' => 'foo',
        ])));
        self::assertSame([], $repository->findBy([
            'id'  => 'one',
            'foo' => 'other',
        ]));
    }

    public function test_id_list_uses_batch_get_and_preserves_requested_order_while_omitting_missing_models(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::never())->method('getItem');
        $client->expects(self::never())->method('query');
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->batchOutput([
            $this->item($this->model('three')),
            $this->item($this->model('one')),
        ]));

        self::assertSame(['one', 'three'], $this->ids($this->repository($client)->findBy([
            'id' => ['one', 'missing', 'three'],
        ])));
    }

    public function test_id_list_predicates_are_applied_only_to_the_batch_loaded_models(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::never())->method('getItem');
        $client->expects(self::never())->method('query');
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->batchOutput([
            $this->item($this->model('two', 'other')),
            $this->item($this->model('one', 'matching')),
        ]));

        self::assertSame(['one'], $this->ids($this->repository($client)->findBy([
            'id'  => ['one', 'two'],
            'foo' => 'matching',
        ])));
    }

    public function test_impossible_and_empty_id_criteria_return_without_calling_dynamo_db(): void
    {
        $client     = $this->clientExpectingNoReads();
        $repository = $this->repository($client);

        self::assertSame([], $repository->findBy([]));
        self::assertSame([], $repository->findBy([
            'id' => null,
        ]));
        self::assertSame([], $repository->findBy([
            'id' => [],
        ]));
    }

    public function test_malformed_id_list_fails_before_calling_dynamo_db(): void
    {
        $repository = $this->repository($this->clientExpectingNoReads());
        $this->expectException(InvalidIdCriterion::class);

        $repository->findBy([
            'id' => ['one', 2],
        ]);
    }

    /**
     * @dataProvider legacyIdFieldNames
     */
    public function test_non_exact_lowercase_id_fields_retain_query_and_matcher_semantics(string $field): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::never())->method('getItem');
        $client->expects(self::never())->method('batchGetItem');
        $client->expects(self::once())->method('query')->willReturn($this->queryOutput($this->model('one')));

        self::assertSame(['one'], $this->ids($this->repository($client)->findBy([
            $field => 'one',
        ])));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public function legacyIdFieldNames(): iterable
    {
        yield 'Id' => ['Id'];
        yield 'ID' => ['ID'];
    }

    private function clientExpectingNoReads(): DynamoDbClient
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::never())->method('getItem');
        $client->expects(self::never())->method('batchGetItem');
        $client->expects(self::never())->method('query');

        return $client;
    }

    private function repository(DynamoDbClient $client): DynamoDbRepository
    {
        $repository = (new DynamoDbRepositoryFactory(
            $client,
            new SimpleInterfaceSerializer(),
            'table',
            new ReadModelSnapshotStore()
        ))->create('repository', RepositoryTestReadModel::class);
        self::assertInstanceOf(DynamoDbRepository::class, $repository);

        return $repository;
    }

    private function model(string $id, string $foo = 'foo'): RepositoryTestReadModel
    {
        return new RepositoryTestReadModel($id, 'name', $foo, []);
    }

    private function getOutput(?RepositoryTestReadModel $model = null): GetItemOutput
    {
        $output = $this->createStub(GetItemOutput::class);
        $output->method('getItem')
            ->willReturn(null === $model ? [] : $this->item($model));

        return $output;
    }

    /**
     * @param array<int, array<string, AttributeValue>> $items
     */
    private function batchOutput(array $items): BatchGetItemOutput
    {
        $output = $this->createStub(BatchGetItemOutput::class);
        $output->method('getResponses')
            ->willReturn([
                'table' => $items,
            ]);
        $output->method('getUnprocessedKeys')
            ->willReturn([]);

        return $output;
    }

    private function queryOutput(RepositoryTestReadModel $model): QueryOutput
    {
        $output = $this->createStub(QueryOutput::class);
        $output->method('getCount')
            ->willReturn(1);
        $output->method('getItems')
            ->willReturn([$this->item($model)]);

        return $output;
    }

    /**
     * @return array<string, AttributeValue>
     */
    private function item(RepositoryTestReadModel $model): array
    {
        return [
            'Id' => new AttributeValue([
                'S' => $model->getId(),
            ]),
            'Data' => new AttributeValue([
                'S' => (new JsonEncoder())->encode((new SimpleInterfaceSerializer())->serialize($model)),
            ]),
        ];
    }

    /**
     * @param Identifiable[] $models
     *
     * @return list<string>
     */
    private function ids(array $models): array
    {
        return array_map(static fn (Identifiable $model): string => $model->getId(), $models);
    }
}
