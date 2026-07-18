<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\Input\BatchGetItemInput;
use AsyncAws\DynamoDb\Result\BatchGetItemOutput;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use AsyncAws\DynamoDb\ValueObject\KeysAndAttributes;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\BatchGetRetriesExhausted;
use chrisjenkinson\DynamoDbReadModel\DynamoDbReadModelStorage;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedEncodedData;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use chrisjenkinson\DynamoDbReadModel\JsonDecoder;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshot;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use JsonException;
use PHPUnit\Framework\TestCase;

final class DynamoDbReadModelStorageBatchGetTest extends TestCase
{
    public function testEmptyIdsReturnWithoutCallingDynamoDb(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::never())->method('batchGetItem');

        self::assertSame([], $this->storage($client)->findMany([]));
    }

    public function testUnorderedResponsesAreReturnedInUniqueRequestedOrderAndMissingIdsAreOmitted(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->with(self::callback(function (BatchGetItemInput $input): bool {
            $keys = $input->getRequestItems()['table']
                ->getKeys();

            return ['one', 'missing', 'three'] === array_map(static fn (array $key): ?string => $key['Id']->getS(), $keys);
        }))->willReturn($this->output([
            $this->item('three'),
            $this->item('one'),
        ]));

        $models = $this->storage($client)
            ->findMany(['one', 'missing', 'three', 'one']);

        self::assertSame(['one', 'three'], array_map(static fn (Identifiable $model): string => $model->getId(), $models));
    }

    public function testOneHundredIdsUseOneRequest(): void
    {
        $ids    = $this->ids(100);
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->with(self::callback(
            static fn (BatchGetItemInput $input): bool => 100 === count($input->getRequestItems()['table']->getKeys())
        ))->willReturn($this->output([]));

        self::assertSame([], $this->storage($client)->findMany($ids));
    }

    public function testOneHundredAndOneIdsUseSequentialRequestsOfOneHundredAndOne(): void
    {
        $sizes  = [];
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(2))->method('batchGetItem')->willReturnCallback(function (BatchGetItemInput $input) use (&$sizes): BatchGetItemOutput {
            $sizes[] = count($input->getRequestItems()['table']->getKeys());

            return $this->output([]);
        });

        self::assertSame([], $this->storage($client)->findMany($this->ids(101)));
        self::assertSame([100, 1], $sizes);
    }

    /**
     * @dataProvider invalidBatchItems
     *
     * @param array<string, AttributeValue> $item
     */
    public function testRejectsMissingOrNonStringIdAndData(array $item): void
    {
        $this->expectException(UnexpectedEncodedData::class);

        $this->storage($this->clientReturning([$item]))->findMany(['id']);
    }

    /**
     * @return iterable<string, array{array<string, AttributeValue>}>
     */
    public function invalidBatchItems(): iterable
    {
        yield 'missing Id' => [[
            'Data' => $this->stringAttribute('{}'),
        ]];
        yield 'non-string Id' => [[
            'Id' => new AttributeValue([
                'N' => '1',
            ]),
            'Data' => $this->stringAttribute('{}'),
        ]];
        yield 'empty Id' => [[
            'Id'   => $this->stringAttribute(''),
            'Data' => $this->stringAttribute('{}'),
        ]];
        yield 'missing Data' => [[
            'Id' => $this->stringAttribute('id'),
        ]];
        yield 'non-string Data' => [[
            'Id'   => $this->stringAttribute('id'),
            'Data' => new AttributeValue([
                'N' => '1',
            ]),
        ]];
    }

    public function testJsonDecodeFailuresPropagate(): void
    {
        $this->expectException(JsonException::class);

        $this->storage($this->clientReturning([$this->rawItem('id', '{')]))->findMany(['id']);
    }

    public function testRejectsUnexpectedSerializedClassBeforeDeserializing(): void
    {
        UnexpectedSerializableReadModel::$deserializeWasCalled = false;
        $item                                                  = $this->rawItem('id', json_encode([
            'class'   => UnexpectedSerializableReadModel::class,
            'payload' => [
                'id' => 'id',
            ],
        ], JSON_THROW_ON_ERROR));

        try {
            $this->storage($this->clientReturning([$item]))->findMany(['id']);
            self::fail('Expected an unexpected read model exception.');
        } catch (UnexpectedReadModel) {
            self::assertFalse(UnexpectedSerializableReadModel::$deserializeWasCalled);
        }
    }

    public function testRejectsNonIdentifiableDeserializedModel(): void
    {
        $item = $this->rawItem('id', json_encode([
            'class'   => NonIdentifiableSerializableReadModel::class,
            'payload' => [
                'id' => 'id',
            ],
        ], JSON_THROW_ON_ERROR));

        $this->expectException(UnexpectedReadModel::class);

        $this->storage($this->clientReturning([$item]), NonIdentifiableSerializableReadModel::class)->findMany(['id']);
    }

    public function testRejectsDeserializedModelOfWrongClass(): void
    {
        $item = $this->rawItem('id', json_encode([
            'class'   => UnexpectedSerializableReadModel::class,
            'payload' => [
                'id' => 'id',
            ],
        ], JSON_THROW_ON_ERROR));

        $this->expectException(UnexpectedReadModel::class);

        $this->storage($this->clientReturning([$item]))->findMany(['id']);
    }

    public function testRejectsPhysicalAndPayloadIdMismatch(): void
    {
        $this->expectException(UnexpectedReadModel::class);

        $this->storage($this->clientReturning([$this->item('payload', 'physical')]))->findMany(['physical']);
    }

    /**
     * @dataProvider invalidResponseMaps
     *
     * @param array<string, array<int, array<string, AttributeValue>>> $responses
     */
    public function testRejectsWrongOrExtraResponseTablesBeforeDeserializing(array $responses): void
    {
        $snapshots = new ReadModelSnapshotStore();
        $client    = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->outputMap($responses));

        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($client, snapshots: $snapshots)
                ->findMany(['one']);
        } finally {
            self::assertFalse($this->hasSnapshot($snapshots, 'one'));
        }
    }

    /**
     * @return iterable<string, array{array<string, array<int, array<string, AttributeValue>>>}>
     */
    public function invalidResponseMaps(): iterable
    {
        yield 'wrong table' => [[
            'other' => [$this->item('one')],
        ]];
        yield 'extra table' => [[
            'table' => [$this->item('one')],
            'other' => [],
        ]];
    }

    public function testRejectsUnknownResponseIdBeforeDeserializing(): void
    {
        $snapshots = new ReadModelSnapshotStore();
        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($this->clientReturning([$this->item('unknown')]), snapshots: $snapshots)->findMany(['one']);
        } finally {
            self::assertFalse($this->hasSnapshot($snapshots, 'unknown'));
        }
    }

    public function testRejectsDuplicateConflictingResponseIdsBeforeDeserializing(): void
    {
        $snapshots = new ReadModelSnapshotStore();
        $client    = $this->clientReturning([
            $this->item('one'),
            $this->item('conflicting-payload', 'one'),
        ]);
        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($client, snapshots: $snapshots)
                ->findMany(['one']);
        } finally {
            self::assertFalse($this->hasSnapshot($snapshots, 'one'));
        }
    }

    public function testRejectsDuplicateResponseIdsBeforeDeserializing(): void
    {
        $snapshots = new ReadModelSnapshotStore();
        $client    = $this->clientReturning([
            $this->item('one'),
            $this->item('one'),
        ]);
        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($client, snapshots: $snapshots)
                ->findMany(['one']);
        } finally {
            self::assertFalse($this->hasSnapshot($snapshots, 'one'));
        }
    }

    public function testRejectsResponseAndUnprocessedOverlapBeforeDeserializing(): void
    {
        $snapshots = new ReadModelSnapshotStore();
        $client    = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->output([$this->item('one')], [
            'table' => $this->keys(['one']),
        ]));
        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($client, snapshots: $snapshots)
                ->findMany(['one']);
        } finally {
            self::assertFalse($this->hasSnapshot($snapshots, 'one'));
        }
    }

    public function testAccumulatesProcessedResponsesAndRetriesOnlyReturnedKeys(): void
    {
        $unprocessed = $this->keys(['two']);
        $outputs     = [
            $this->output([$this->item('one')], [
                'table' => $unprocessed,
            ]),
            $this->output([$this->item('two')]),
        ];
        $requests = [];
        $client   = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(2))->method('batchGetItem')->willReturnCallback(function (BatchGetItemInput $input) use (&$outputs, &$requests): BatchGetItemOutput {
            $requests[] = $input;
            self::assertNotEmpty($outputs);

            return array_shift($outputs);
        });
        $caps = [];

        $models = $this->storage($client, delay: static function (int $cap) use (&$caps): void {
            $caps[] = $cap;
        })->findMany(['one', 'two']);

        self::assertSame(['one', 'two'], array_map(static fn (Identifiable $model): string => $model->getId(), $models));
        self::assertSame([25000], $caps);
        self::assertSame($unprocessed, $requests[1]->getRequestItems()['table']);
    }

    public function testRetryDelayCapsResetForEachChunkAndThereIsNoDelayAfterCompletion(): void
    {
        $outputs = [
            $this->output([], [
                'table' => $this->keys(['id-001']),
            ]),
            $this->output([]),
            $this->output([], [
                'table' => $this->keys(['id-101']),
            ]),
            $this->output([]),
        ];
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(4))->method('batchGetItem')->willReturnCallback(static function () use (&$outputs): BatchGetItemOutput {
            self::assertNotEmpty($outputs);

            return array_shift($outputs);
        });
        $caps = [];

        $this->storage($client, delay: static function (int $cap) use (&$caps): void {
            $caps[] = $cap;
        })->findMany($this->ids(101));

        self::assertSame([25000, 25000], $caps);
    }

    public function testThrowsAfterExactlyFourAttemptsAndDoesNotRequestALaterChunk(): void
    {
        $ids         = $this->ids(101);
        $unprocessed = $this->keys(['id-002']);
        $output      = $this->output([], [
            'table' => $unprocessed,
        ]);
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(4))->method('batchGetItem')->willReturn($output);
        $caps = [];

        try {
            $this->storage($client, delay: static function (int $cap) use (&$caps): void {
                $caps[] = $cap;
            })->findMany($ids);
            self::fail('Expected retries to be exhausted.');
        } catch (BatchGetRetriesExhausted $exception) {
            self::assertSame('table', $exception->table);
            self::assertSame('repository', $exception->repositoryName);
            self::assertSame(RepositoryTestReadModel::class, $exception->readModelClass);
            self::assertSame(['id-002'], $exception->unresolvedIds);
            self::assertSame(4, $exception->attempts);
            self::assertNull($exception->getPrevious());
            self::assertSame([25000, 50000, 100000], $caps);
        }
    }

    public function testExhaustedIdsUseOriginalChunkOrderRatherThanReturnedKeyOrder(): void
    {
        $output = $this->output([], [
            'table' => $this->keys(['three', 'one']),
        ]);
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(4))->method('batchGetItem')->willReturn($output);

        try {
            $this->storage($client, delay: static function (int $cap): void {
            })->findMany(['one', 'two', 'three']);
            self::fail('Expected retries to be exhausted.');
        } catch (BatchGetRetriesExhausted $exception) {
            self::assertSame(['one', 'three'], $exception->unresolvedIds);
        }
    }

    /**
     * @dataProvider malformedUnprocessedKeys
     *
     * @param array<string, KeysAndAttributes> $unprocessed
     */
    public function testRejectsMalformedUnprocessedKeysBeforeDelayOrAnotherRequest(array $unprocessed): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->output([], $unprocessed));
        $delayCalled = false;

        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($client, delay: static function (int $cap) use (&$delayCalled): void {
                $delayCalled = true;
            })->findMany(['one', 'two']);
        } finally {
            self::assertFalse($delayCalled);
        }
    }

    /**
     * @return iterable<string, array{array<string, KeysAndAttributes>}>
     */
    public function malformedUnprocessedKeys(): iterable
    {
        yield 'unknown table' => [[
            'other' => $this->keys(['one']),
        ]];
        yield 'multiple tables' => [[
            'table' => $this->keys(['one']),
            'other' => $this->keys(['two']),
        ]];
        yield 'configured table with zero keys' => [[
            'table' => new KeysAndAttributes([
                'Keys' => [],
            ]),
        ]];
        yield 'missing Name' => [[
            'table' => $this->rawKeys([[
                'Id' => $this->stringAttribute('one'),
            ]]),
        ]];
        yield 'non-string Name' => [[
            'table' => $this->rawKeys([[
                'Name' => new AttributeValue([
                    'N' => '1',
                ]),
                'Id' => $this->stringAttribute('one'),
            ]]),
        ]];
        yield 'wrong Name' => [[
            'table' => $this->rawKeys([[
                'Name' => $this->stringAttribute('other'),
                'Id'   => $this->stringAttribute('one'),
            ]]),
        ]];
        yield 'missing Id' => [[
            'table' => $this->rawKeys([[
                'Name' => $this->stringAttribute('repository'),
            ]]),
        ]];
        yield 'non-string Id' => [[
            'table' => $this->rawKeys([[
                'Name' => $this->stringAttribute('repository'),
                'Id'   => new AttributeValue([
                    'N' => '1',
                ]),
            ]]),
        ]];
        yield 'empty Id' => [[
            'table' => $this->keys(['']),
        ]];
        yield 'unknown Id' => [[
            'table' => $this->keys(['unknown']),
        ]];
        yield 'duplicate Id' => [[
            'table' => $this->keys(['one', 'one']),
        ]];
    }

    public function testDelayExceptionsPropagateWithoutAnotherRequest(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->output([], [
            'table' => $this->keys(['one']),
        ]));
        $expected = new \RuntimeException('delay failed');

        try {
            $this->storage($client, delay: static function (int $cap) use ($expected): void {
                throw $expected;
            })->findMany(['one']);
            self::fail('Expected delay failure.');
        } catch (\RuntimeException $actual) {
            self::assertSame($expected, $actual);
        }
    }

    public function testSnapshotsFromCompletedChunksRemainAfterALaterChunkExhaustsRetries(): void
    {
        $firstChunk  = $this->ids(100);
        $secondChunk = ['id-101'];
        $outputs     = [
            $this->output([$this->item('id-001')]),
            $this->output([], [
                'table' => $this->keys($secondChunk),
            ]),
            $this->output([], [
                'table' => $this->keys($secondChunk),
            ]),
            $this->output([], [
                'table' => $this->keys($secondChunk),
            ]),
            $this->output([], [
                'table' => $this->keys($secondChunk),
            ]),
        ];
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(5))->method('batchGetItem')->willReturnCallback(static function () use (&$outputs): BatchGetItemOutput {
            self::assertNotEmpty($outputs);

            return array_shift($outputs);
        });
        $client->expects(self::never())->method('putItem');
        $storage = $this->storage($client, delay: static function (int $cap): void {
        });

        try {
            $storage->findMany([...$firstChunk, ...$secondChunk]);
            self::fail('Expected retries to be exhausted.');
        } catch (BatchGetRetriesExhausted) {
        }

        $model = new RepositoryTestReadModel('id-001', 'name', 'foo', []);
        $storage->save($storage->prepareSave($model));
    }

    public function testRetriesValidateAgainstOnlyThePreviousAttemptsOutstandingIds(): void
    {
        $outputs = [
            $this->output([], [
                'table' => $this->keys(['two']),
            ]),
            $this->output([], [
                'table' => $this->keys(['one']),
            ]),
        ];
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::exactly(2))->method('batchGetItem')->willReturnCallback(static function () use (&$outputs): BatchGetItemOutput {
            self::assertNotEmpty($outputs);

            return array_shift($outputs);
        });
        $caps = [];
        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($client, delay: static function (int $cap) use (&$caps): void {
                $caps[] = $cap;
            })->findMany(['one', 'two']);
        } finally {
            self::assertSame([25000], $caps);
        }
    }

    /**
     * @param array<int, array<string, AttributeValue>> $items
     */
    private function clientReturning(array $items): DynamoDbClient
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->output($items));

        return $client;
    }

    /**
     * @param array<int, array<string, AttributeValue>> $items
     * @param array<string, KeysAndAttributes>          $unprocessed
     */
    private function output(array $items, array $unprocessed = []): BatchGetItemOutput
    {
        return $this->outputMap([] === $items ? [] : [
            'table' => $items,
        ], $unprocessed);
    }

    /**
     * @param array<string, array<int, array<string, AttributeValue>>> $responses
     * @param array<string, KeysAndAttributes>                         $unprocessed
     */
    private function outputMap(array $responses, array $unprocessed = []): BatchGetItemOutput
    {
        $output = $this->createStub(BatchGetItemOutput::class);
        $output->method('getResponses')
            ->willReturn($responses);
        $output->method('getUnprocessedKeys')
            ->willReturn($unprocessed);

        return $output;
    }

    private function hasSnapshot(ReadModelSnapshotStore $snapshots, string $id): bool
    {
        $model = new RepositoryTestReadModel($id, 'name', 'foo', []);

        return $snapshots->hasSameSnapshot(new ReadModelSnapshot(
            'table',
            'repository',
            RepositoryTestReadModel::class,
            $id,
            (new SimpleInterfaceSerializer())->serialize($model)
        ));
    }

    /**
     * @return array<string, AttributeValue>
     */
    private function item(string $payloadId, ?string $physicalId = null): array
    {
        $model = new RepositoryTestReadModel($payloadId, 'name', 'foo', []);

        return $this->rawItem($physicalId ?? $payloadId, (new JsonEncoder())->encode((new SimpleInterfaceSerializer())->serialize($model)));
    }

    /**
     * @return array<string, AttributeValue>
     */
    private function rawItem(string $physicalId, string $data): array
    {
        return [
            'Id'   => $this->stringAttribute($physicalId),
            'Data' => $this->stringAttribute($data),
        ];
    }

    private function stringAttribute(string $value): AttributeValue
    {
        return new AttributeValue([
            'S' => $value,
        ]);
    }

    /**
     * @param list<string> $ids
     */
    private function keys(array $ids): KeysAndAttributes
    {
        return $this->rawKeys(array_map(fn (string $id): array => [
            'Name' => $this->stringAttribute('repository'),
            'Id'   => $this->stringAttribute($id),
        ], $ids));
    }

    /**
     * @param array<int, array<string, AttributeValue>> $keys
     */
    private function rawKeys(array $keys): KeysAndAttributes
    {
        return new KeysAndAttributes([
            'Keys' => $keys,
        ]);
    }

    /**
     * @return list<string>
     */
    private function ids(int $count): array
    {
        return array_map(static fn (int $number): string => sprintf('id-%03d', $number), range(1, $count));
    }

    private function storage(
        DynamoDbClient $client,
        string $class = RepositoryTestReadModel::class,
        ?\Closure $delay = null,
        ?ReadModelSnapshotStore $snapshots = null
    ): DynamoDbReadModelStorage {
        return new DynamoDbReadModelStorage(
            $client,
            new InputBuilder(),
            new SimpleInterfaceSerializer(),
            new JsonEncoder(),
            new JsonDecoder(),
            'table',
            'repository',
            $class,
            $snapshots ?? new ReadModelSnapshotStore(),
            $delay
        );
    }
}
