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
    public function test_empty_ids_return_without_calling_dynamo_db(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::never())->method('batchGetItem');

        self::assertSame([], $this->storage($client)->findMany([]));
    }

    public function test_unordered_responses_are_returned_in_unique_requested_order_and_missing_ids_are_omitted(): void
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

    public function test_one_hundred_ids_use_one_request(): void
    {
        $ids    = $this->ids(100);
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->with(self::callback(
            static fn (BatchGetItemInput $input): bool => 100 === count($input->getRequestItems()['table']->getKeys())
        ))->willReturn($this->output([]));

        self::assertSame([], $this->storage($client)->findMany($ids));
    }

    public function test_one_hundred_and_one_ids_use_sequential_requests_of_one_hundred_and_one(): void
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
    public function test_rejects_invalid_id_or_data_attributes(array $item, string $message): void
    {
        $this->expectException(UnexpectedEncodedData::class);
        $this->expectExceptionMessage($message);

        $this->storage($this->clientReturning([$item]))->findMany(['id']);
    }

    /**
     * @return iterable<string, array{array<string, AttributeValue>, string}>
     */
    public function invalidBatchItems(): iterable
    {
        yield 'missing Id' => [[
            'Data' => $this->stringAttribute('{}'),
        ], 'Expected "Id" to be "string", instead got "null".'];
        yield 'non-string Id' => [[
            'Id' => new AttributeValue([
                'N' => '1',
            ]),
            'Data' => $this->stringAttribute('{}'),
        ], 'Expected "Id" to be "string", instead got "null".'];
        yield 'empty Id' => [[
            'Id'   => $this->stringAttribute(''),
            'Data' => $this->stringAttribute('{}'),
        ], 'Expected "Id" to be "a unique non-empty id outstanding in the current attempt", instead got "string".'];
        yield 'missing Data' => [[
            'Id' => $this->stringAttribute('id'),
        ], 'Expected "Data" to be "string", instead got "null".'];
        yield 'non-string Data' => [[
            'Id'   => $this->stringAttribute('id'),
            'Data' => new AttributeValue([
                'N' => '1',
            ]),
        ], 'Expected "Data" to be "string", instead got "null".'];
    }

    public function test_json_decode_failures_propagate(): void
    {
        $this->expectException(JsonException::class);

        $this->storage($this->clientReturning([$this->rawItem('id', '{')]))->findMany(['id']);
    }

    public function test_rejects_unexpected_serialized_class_before_deserializing(): void
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

    public function test_rejects_non_identifiable_deserialized_model(): void
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

    public function test_rejects_deserialized_model_of_wrong_class(): void
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

    public function test_rejects_physical_and_payload_id_mismatch(): void
    {
        $this->expectException(UnexpectedReadModel::class);
        $this->expectExceptionMessage(sprintf(
            'Mismatch between data (%s with id payload) and expected class (%s with id physical)',
            RepositoryTestReadModel::class,
            RepositoryTestReadModel::class
        ));

        $this->storage($this->clientReturning([$this->item('payload', 'physical')]))->findMany(['physical']);
    }

    /**
     * @dataProvider invalidResponseMaps
     *
     * @param array<string, array<int, array<string, AttributeValue>>> $responses
     */
    public function test_rejects_wrong_or_extra_response_tables_before_deserializing(array $responses): void
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

    public function test_rejects_unknown_response_id_before_deserializing(): void
    {
        $snapshots = new ReadModelSnapshotStore();
        $this->expectException(UnexpectedEncodedData::class);

        try {
            $this->storage($this->clientReturning([$this->item('unknown')]), snapshots: $snapshots)->findMany(['one']);
        } finally {
            self::assertFalse($this->hasSnapshot($snapshots, 'unknown'));
        }
    }

    public function test_rejects_duplicate_conflicting_response_ids_before_deserializing(): void
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

    public function test_rejects_duplicate_response_ids_before_deserializing(): void
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

    public function test_rejects_response_and_unprocessed_overlap_before_deserializing(): void
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

    public function test_accumulates_processed_responses_and_retries_only_returned_keys(): void
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

    public function test_retry_delay_caps_reset_for_each_chunk_and_there_is_no_delay_after_completion(): void
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

    public function test_throws_after_exactly_four_attempts_and_does_not_request_a_later_chunk(): void
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
            self::assertSame(
                'Batch get retries exhausted for repository "repository" in table "table" after 4 attempts; unresolved ids: id-002.',
                $exception->getMessage()
            );
            self::assertSame('table', $exception->table);
            self::assertSame('repository', $exception->repositoryName);
            self::assertSame(RepositoryTestReadModel::class, $exception->readModelClass);
            self::assertSame(['id-002'], $exception->unresolvedIds);
            self::assertSame(4, $exception->attempts);
            self::assertNull($exception->getPrevious());
            self::assertSame([25000, 50000, 100000], $caps);
        }
    }

    public function test_exhausted_ids_use_original_chunk_order_rather_than_returned_key_order(): void
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
    public function test_rejects_malformed_unprocessed_keys_before_delay_or_another_request(array $unprocessed): void
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

    public function test_missing_unprocessed_name_reports_the_malformed_attribute(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->output([], [
            'table' => $this->rawKeys([[
                'Id' => $this->stringAttribute('one'),
            ]]),
        ]));

        $this->expectException(UnexpectedEncodedData::class);
        $this->expectExceptionMessage('Expected "UnprocessedKeys.Name" to be "string", instead got "null".');

        $this->storage($client)
            ->findMany(['one']);
    }

    public function test_missing_unprocessed_id_reports_the_malformed_attribute(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects(self::once())->method('batchGetItem')->willReturn($this->output([], [
            'table' => $this->rawKeys([[
                'Name' => $this->stringAttribute('repository'),
            ]]),
        ]));

        $this->expectException(UnexpectedEncodedData::class);
        $this->expectExceptionMessage('Expected "UnprocessedKeys.Id" to be "string", instead got "null".');

        $this->storage($client)
            ->findMany(['one']);
    }

    public function test_delay_exceptions_propagate_without_another_request(): void
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

    public function test_snapshots_from_completed_chunks_remain_after_a_later_chunk_exhausts_retries(): void
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

    public function test_retries_validate_against_only_the_previous_attempts_outstanding_ids(): void
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
