<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\Core\Response;
use AsyncAws\Core\Test\Http\SimpleMockedResponse;
use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\Input\PutItemInput;
use AsyncAws\DynamoDb\Result\DeleteItemOutput;
use AsyncAws\DynamoDb\Result\GetItemOutput;
use AsyncAws\DynamoDb\Result\PutItemOutput;
use AsyncAws\DynamoDb\Result\QueryOutput;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use Broadway\ReadModel\Repository;
use Broadway\ReadModel\SerializableReadModel;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DeferredDynamoDbRepository;
use chrisjenkinson\DynamoDbReadModel\DeferredFlushFailed;
use chrisjenkinson\DynamoDbReadModel\DeferredOperation;
use chrisjenkinson\DynamoDbReadModel\DeferredRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\DynamoDbReadModelStorage;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepositoryFactory;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\FlushableRepository;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use chrisjenkinson\DynamoDbReadModel\JsonDecoder;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelFieldMatcher;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\MockHttpClient;

final class DeferredDynamoDbRepositoryTest extends TestCase
{
    public function test_it_is_a_broadway_repository_with_an_explicit_flush_boundary(): void
    {
        $repository = $this->createRepository($this->createMock(DynamoDbClient::class));

        self::assertInstanceOf(Repository::class, $repository);
        self::assertInstanceOf(FlushableRepository::class, $repository);
    }

    public function test_factory_creation_is_explicitly_deferred(): void
    {
        $factory = new DynamoDbRepositoryFactory(
            $this->createMock(DynamoDbClient::class),
            new SimpleInterfaceSerializer(),
            'table',
            new ReadModelSnapshotStore()
        );

        self::assertNotInstanceOf(FlushableRepository::class, $factory->create('name', RepositoryTestReadModel::class));
        self::assertInstanceOf(DeferredRepositoryFactory::class, $factory);
        self::assertInstanceOf(FlushableRepository::class, $factory->createDeferred('name', RepositoryTestReadModel::class));
    }

    public function test_find_uses_an_identity_map_before_reading_from_dynamodb(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('id', 'name', 'foo', [])));

        $repository = $this->createRepository($client);

        $first  = $repository->find('id');
        $second = $repository->find('id');

        self::assertInstanceOf(RepositoryTestReadModel::class, $first);
        self::assertSame($first, $second);
    }

    public function test_find_accepts_a_stringable_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('getItem');

        $repository = $this->createRepository($client);
        $model      = new RepositoryTestReadModel('id', 'name', 'foo', []);

        $repository->save($model);

        self::assertEquals($model, $repository->find($this->stringableId('id')));
    }

    public function test_save_marks_the_model_dirty_without_writing_immediately(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('putItem');

        $this->createRepository($client)->save(new RepositoryTestReadModel('id', 'name', 'foo', []));
    }

    public function test_save_rejects_a_read_model_for_a_different_repository_class(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('putItem');

        $this->expectException(UnexpectedReadModel::class);

        $this->createRepository($client)->save(new UnexpectedSerializableReadModel('wrong-class'));
    }

    public function test_flush_persists_dirty_models(): void
    {
        $putItemOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));

        $repository->flush();

        self::assertTrue($putItemOutput->info()['resolved']);
    }

    public function test_repeated_saves_collapse_to_one_final_write_on_flush(): void
    {
        $putItemOutput   = new PutItemOutput($this->createResponse());
        $writtenPayloads = [];

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('putItem')
            ->willReturnCallback(function ($input) use (&$writtenPayloads, $putItemOutput): PutItemOutput {
                $writtenPayloads[] = $this->decodePutItemPayload($input);

                return $putItemOutput;
            });

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('id', 'first', 'foo', []));
        $repository->save(new RepositoryTestReadModel('id', 'second', 'foo', []));

        $repository->flush();

        self::assertSame('second', $writtenPayloads[0]['payload']['name']);
    }

    public function test_save_captures_the_model_state_when_save_is_called(): void
    {
        $putItemOutput   = new PutItemOutput($this->createResponse());
        $writtenPayloads = [];

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('putItem')
            ->willReturnCallback(function ($input) use (&$writtenPayloads, $putItemOutput): PutItemOutput {
                $writtenPayloads[] = $this->decodePutItemPayload($input);

                return $putItemOutput;
            });

        $repository = $this->createMutableRepository($client);
        $model      = new MutableRepositoryTestReadModel('id', 'saved');

        $repository->save($model);
        $model->rename('mutated');

        $repository->flush();

        self::assertSame('saved', $writtenPayloads[0]['payload']['name']);
    }

    public function test_find_returns_the_saved_snapshot_state_after_the_original_model_is_mutated(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('getItem');

        $repository = $this->createMutableRepository($client);
        $model      = new MutableRepositoryTestReadModel('id', 'saved');

        $repository->save($model);
        $model->rename('mutated');

        $found = $repository->find('id');

        self::assertInstanceOf(MutableRepositoryTestReadModel::class, $found);
        self::assertSame('saved', $found->getName());
    }

    public function test_pending_save_operations_expose_the_saved_snapshot_state(): void
    {
        $repository = $this->createMutableRepository($this->createMock(DynamoDbClient::class));
        $model      = new MutableRepositoryTestReadModel('id', 'saved');

        $repository->save($model);
        $model->rename('mutated');

        $operations = $repository->pendingOperations();

        self::assertCount(1, $operations);
        self::assertInstanceOf(MutableRepositoryTestReadModel::class, $operations[0]->model);
        self::assertSame('saved', $operations[0]->model->getName());
    }

    public function test_pending_save_operations_ignore_mutations_to_the_identity_mapped_snapshot(): void
    {
        $repository = $this->createMutableRepository($this->createMock(DynamoDbClient::class));
        $model      = new MutableRepositoryTestReadModel('id', 'saved');

        $repository->save($model);

        $found = $repository->find('id');
        self::assertInstanceOf(MutableRepositoryTestReadModel::class, $found);
        $found->rename('mutated');

        $operations = $repository->pendingOperations();

        self::assertCount(1, $operations);
        self::assertInstanceOf(MutableRepositoryTestReadModel::class, $operations[0]->model);
        self::assertSame('saved', $operations[0]->model->getName());
    }

    public function test_find_all_preserves_the_identity_mapped_staged_save_snapshot(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor());

        $repository = $this->createMutableRepository($client);
        $repository->save(new MutableRepositoryTestReadModel('id', 'saved'));

        $found = $repository->find('id');
        self::assertInstanceOf(MutableRepositoryTestReadModel::class, $found);
        $found->rename('mutated');

        $all = $repository->findAll();

        self::assertCount(1, $all);
        self::assertSame($found, $all[0]);
        self::assertSame('mutated', $all[0]->getName());
    }

    public function test_remove_marks_the_model_removed_without_deleting_immediately(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('deleteItem');

        $this->createRepository($client)->remove('id');
    }

    public function test_remove_accepts_a_stringable_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('getItem');

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));
        $repository->remove($this->stringableId('id'));

        self::assertNull($repository->find('id'));
    }

    public function test_removed_items_are_hidden_from_find_until_saved_again(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('getItem');

        $repository = $this->createRepository($client);
        $repository->remove('id');

        self::assertNull($repository->find('id'));
    }

    public function test_flush_persists_pending_removes(): void
    {
        $deleteItemOutput = new DeleteItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('deleteItem')->willReturn($deleteItemOutput);

        $repository = $this->createRepository($client);
        $repository->remove('id');

        $repository->flush();

        self::assertTrue($deleteItemOutput->info()['resolved']);
    }

    public function test_flush_deletes_each_removed_id(): void
    {
        $deletedIds = [];

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->exactly(2))
            ->method('deleteItem')
            ->willReturnCallback(function ($input) use (&$deletedIds): DeleteItemOutput {
                $deletedIds[] = $input->getKey()['Id']->getS();

                return new DeleteItemOutput($this->createResponse());
            });

        $repository = $this->createRepository($client);
        $repository->remove('first');
        $repository->remove('second');

        $repository->flush();

        self::assertSame(['first', 'second'], $deletedIds);
    }

    public function test_saving_a_removed_item_cancels_the_pending_remove(): void
    {
        $putItemOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('deleteItem');
        $client->expects($this->once())->method('putItem')->willReturn($putItemOutput);

        $repository = $this->createRepository($client);
        $repository->remove('id');
        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));

        $repository->flush();
    }

    public function test_removing_a_saved_item_flushes_a_delete(): void
    {
        $deleteItemOutput = new DeleteItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('putItem');
        $client->expects($this->once())->method('deleteItem')->willReturn($deleteItemOutput);

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));
        $repository->remove('id');

        $repository->flush();
    }

    public function test_pending_operations_are_available_for_inspection(): void
    {
        $repository = $this->createRepository($this->createMock(DynamoDbClient::class));
        $repository->save(new RepositoryTestReadModel('saved', 'name', 'foo', []));
        $repository->remove('removed');

        $operations = $repository->pendingOperations();

        self::assertCount(2, $operations);
        self::assertSame(DeferredOperation::REMOVE, $operations[0]->operation);
        self::assertSame('removed', $operations[0]->id);
        self::assertSame(DeferredOperation::SAVE, $operations[1]->operation);
        self::assertSame('saved', $operations[1]->id);
    }

    public function test_clear_discards_managed_dirty_and_removed_state(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->never())->method('putItem');
        $client->expects($this->never())->method('deleteItem');
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('removed', 'name', 'foo', [])));

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('dirty', 'name', 'foo', []));
        $repository->remove('removed');

        $repository->clear();
        $repository->flush();

        self::assertInstanceOf(RepositoryTestReadModel::class, $repository->find('removed'));
    }

    public function test_find_all_overlays_dirty_and_removed_state_on_persisted_models(): void
    {
        $persisted = new RepositoryTestReadModel('persisted', 'persisted', 'foo', []);
        $dirty     = new RepositoryTestReadModel('dirty', 'dirty', 'foo', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($persisted));

        $repository = $this->createRepository($client);
        $repository->remove('persisted');
        $repository->save($dirty);

        self::assertEquals([$dirty], $repository->findAll());
    }

    public function test_find_all_continues_past_removed_persisted_models(): void
    {
        $removed = new RepositoryTestReadModel('removed', 'removed', 'foo', []);
        $kept    = new RepositoryTestReadModel('kept', 'kept', 'foo', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($removed, $kept));

        $repository = $this->createRepository($client);
        $repository->remove('removed');

        self::assertEquals([$kept], $repository->findAll());
    }

    public function test_find_rejects_rows_whose_physical_id_does_not_match_the_payload_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('getItem')
            ->willReturn($this->getItemOutputFor(new RepositoryTestReadModel('payload-id', 'name', 'foo', [])));

        $this->expectException(UnexpectedReadModel::class);

        $this->createRepository($client)->find('physical-id');
    }

    public function test_find_all_rejects_rows_whose_physical_id_does_not_match_the_payload_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('query')
            ->willReturn($this->queryOutputForPhysicalId('physical-id', new RepositoryTestReadModel('payload-id', 'name', 'foo', [])));

        $this->expectException(UnexpectedReadModel::class);

        $this->createRepository($client)->findAll();
    }

    public function test_find_all_preserves_the_identity_mapped_model_when_persisted_state_is_reloaded(): void
    {
        $first  = new RepositoryTestReadModel('id', 'first', 'foo', []);
        $second = new RepositoryTestReadModel('id', 'second', 'foo', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($second));

        $repository = $this->createRepository($client);
        $repository->save($first);

        self::assertEquals($first, $repository->findAll()[0]);
    }

    public function test_find_all_preserves_the_identity_mapped_model_loaded_by_find_when_persisted_state_is_reloaded(): void
    {
        $first  = new RepositoryTestReadModel('id', 'first', 'foo', []);
        $second = new RepositoryTestReadModel('id', 'second', 'foo', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('getItem')->willReturn($this->getItemOutputFor($first));
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($second));

        $repository = $this->createRepository($client);
        $model      = $repository->find('id');

        self::assertSame($model, $repository->findAll()[0]);
    }

    public function test_find_by_filters_the_deferred_view(): void
    {
        $persisted = new RepositoryTestReadModel('persisted', 'persisted', 'unmatched', []);
        $dirty     = new RepositoryTestReadModel('dirty', 'dirty', 'matched', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($persisted));

        $repository = $this->createRepository($client);
        $repository->save($dirty);

        self::assertEquals([$dirty], $repository->findBy([
            'foo' => 'matched',
        ]));
    }

    public function test_find_by_excludes_pending_saves_that_do_not_match_the_filter(): void
    {
        $persisted = new RepositoryTestReadModel('persisted', 'persisted', 'matched', []);
        $dirty     = new RepositoryTestReadModel('dirty', 'dirty', 'unmatched', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($persisted));

        $repository = $this->createRepository($client);
        $repository->save($dirty);

        self::assertEquals([$persisted], $repository->findBy([
            'foo' => 'matched',
        ]));
    }

    public function test_find_by_excludes_persisted_models_updated_in_memory_to_no_longer_match(): void
    {
        $persisted = new RepositoryTestReadModel('id', 'name', 'matched', []);
        $dirty     = new RepositoryTestReadModel('id', 'name', 'unmatched', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($persisted));

        $repository = $this->createRepository($client);
        $repository->save($dirty);

        self::assertSame([], $repository->findBy([
            'foo' => 'matched',
        ]));
    }

    public function test_find_by_includes_persisted_models_updated_in_memory_to_match(): void
    {
        $persisted = new RepositoryTestReadModel('id', 'name', 'unmatched', []);
        $dirty     = new RepositoryTestReadModel('id', 'name', 'matched', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($persisted));

        $repository = $this->createRepository($client);
        $repository->save($dirty);

        self::assertEquals([$dirty], $repository->findBy([
            'foo' => 'matched',
        ]));
    }

    public function test_find_by_excludes_models_removed_by_id(): void
    {
        $persisted = new RepositoryTestReadModel('id', 'name', 'matched', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($persisted));

        $repository = $this->createRepository($client);
        $repository->remove('id');

        self::assertSame([], $repository->findBy([
            'foo' => 'matched',
        ]));
    }

    public function test_find_by_rejects_rows_whose_physical_id_does_not_match_the_payload_id(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('query')
            ->willReturn($this->queryOutputForPhysicalId('physical-id', new RepositoryTestReadModel('payload-id', 'name', 'matched', [])));

        $this->expectException(UnexpectedReadModel::class);

        $this->createRepository($client)->findBy([
            'foo' => 'matched',
        ]);
    }

    public function test_find_by_filters_array_getters_by_contained_values(): void
    {
        $matching    = new RepositoryTestReadModel('matching', 'matching', 'foo', ['matched']);
        $notMatching = new RepositoryTestReadModel('not-matching', 'not-matching', 'foo', ['other']);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($matching, $notMatching));

        $repository = $this->createRepository($client);

        self::assertEquals([$matching], $repository->findBy([
            'array' => 'matched',
        ]));
    }

    public function test_find_by_uses_public_getter_naming(): void
    {
        $matching    = new RepositoryTestReadModel('matching', 'matched', 'foo', []);
        $notMatching = new RepositoryTestReadModel('not-matching', 'other', 'foo', []);

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('query')->willReturn($this->queryOutputFor($matching, $notMatching));

        $repository = $this->createRepository($client);

        self::assertEquals([$matching], $repository->findBy([
            'name' => 'matched',
        ]));
    }

    public function test_failed_flush_preserves_pending_dirty_state_for_retry(): void
    {
        $successfulOutput = new PutItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->exactly(2))
            ->method('putItem')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new \RuntimeException('failed')),
                $successfulOutput
            );

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));

        try {
            $repository->flush();
            self::fail('Expected the first flush to fail.');
        } catch (\RuntimeException) {
        }

        $repository->flush();
    }

    public function test_failed_flush_reports_the_failed_dirty_operation(): void
    {
        $previous = new \RuntimeException('failed');

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('putItem')
            ->willThrowException($previous);

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));

        try {
            $repository->flush();
            self::fail('Expected flush to fail.');
        } catch (DeferredFlushFailed $exception) {
            self::assertSame(0, $exception->getCode());
            self::assertSame($previous, $exception->getPrevious());
            self::assertSame(DeferredOperation::SAVE, $exception->operation);
            self::assertSame('id', $exception->id);
            self::assertSame('table', $exception->table);
            self::assertSame('name', $exception->repositoryName);
            self::assertSame(RepositoryTestReadModel::class, $exception->readModelClass);
        }
    }

    public function test_failed_flush_reports_caller_supplied_context(): void
    {
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('putItem')
            ->willThrowException(new \RuntimeException('failed'));

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('id', 'name', 'foo', []));

        try {
            $repository->flushWithContext([
                'tenantId'      => 'tenant-1',
                'projector'     => 'Projector',
                'sourceEventId' => 'event-1',
            ]);
            self::fail('Expected flush to fail.');
        } catch (DeferredFlushFailed $exception) {
            self::assertSame([
                'tenantId'      => 'tenant-1',
                'projector'     => 'Projector',
                'sourceEventId' => 'event-1',
            ], $exception->context);
        }
    }

    public function test_failed_flush_after_successful_dirty_write_retries_only_remaining_dirty_models(): void
    {
        $attemptedIds = [];

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->exactly(4))
            ->method('putItem')
            ->willReturnCallback(function ($input) use (&$attemptedIds): PutItemOutput {
                $id             = $input->getItem()['Id']->getS();
                $attemptedIds[] = $id;

                if ('second' === $id && 2 === count($attemptedIds)) {
                    throw new \RuntimeException('failed second');
                }

                return new PutItemOutput($this->createResponse());
            });

        $repository = $this->createRepository($client);
        $repository->save(new RepositoryTestReadModel('first', 'first', 'foo', []));
        $repository->save(new RepositoryTestReadModel('second', 'second', 'foo', []));
        $repository->save(new RepositoryTestReadModel('third', 'third', 'foo', []));

        try {
            $repository->flush();
            self::fail('Expected flush to fail.');
        } catch (DeferredFlushFailed) {
        }

        $operations = $repository->pendingOperations();
        self::assertCount(2, $operations);
        self::assertSame(['second', 'third'], array_map(
            static fn (DeferredOperation $operation): string => $operation->id,
            $operations
        ));

        $repository->flush();

        self::assertSame(['first', 'second', 'second', 'third'], $attemptedIds);
    }

    public function test_failed_flush_preserves_pending_removed_state_for_retry(): void
    {
        $successfulOutput = new DeleteItemOutput($this->createResponse());

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->exactly(2))
            ->method('deleteItem')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new \RuntimeException('failed')),
                $successfulOutput
            );

        $repository = $this->createRepository($client);
        $repository->remove('id');

        try {
            $repository->flush();
            self::fail('Expected the first flush to fail.');
        } catch (\RuntimeException) {
        }

        $operations = $repository->pendingOperations();
        self::assertCount(1, $operations);
        self::assertSame(DeferredOperation::REMOVE, $operations[0]->operation);
        self::assertSame('id', $operations[0]->id);

        $repository->flush();
    }

    public function test_failed_flush_after_successful_remove_keeps_only_later_operations_pending(): void
    {
        $attemptedRemoves = [];

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->exactly(4))
            ->method('deleteItem')
            ->willReturnCallback(function ($input) use (&$attemptedRemoves): DeleteItemOutput {
                $id                 = $input->getKey()['Id']->getS();
                $attemptedRemoves[] = $id;

                if ('second' === $id && 2 === count($attemptedRemoves)) {
                    throw new \RuntimeException('failed second');
                }

                return new DeleteItemOutput($this->createResponse());
            });

        $repository = $this->createRepository($client);
        $repository->remove('first');
        $repository->remove('second');
        $repository->remove('third');

        try {
            $repository->flush();
            self::fail('Expected flush to fail.');
        } catch (DeferredFlushFailed) {
        }

        $operations = $repository->pendingOperations();
        self::assertCount(2, $operations);
        self::assertSame(['second', 'third'], array_map(
            static fn (DeferredOperation $operation): string => $operation->id,
            $operations
        ));

        $repository->flush();

        self::assertSame(['first', 'second', 'second', 'third'], $attemptedRemoves);
    }

    public function test_failed_flush_reports_the_failed_remove_operation(): void
    {
        $previous = new \RuntimeException('failed');

        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())
            ->method('deleteItem')
            ->willThrowException($previous);

        $repository = $this->createRepository($client);
        $repository->remove('id');

        try {
            $repository->flush();
            self::fail('Expected flush to fail.');
        } catch (DeferredFlushFailed $exception) {
            self::assertSame($previous, $exception->getPrevious());
            self::assertSame(DeferredOperation::REMOVE, $exception->operation);
            self::assertSame('id', $exception->id);
            self::assertSame('table', $exception->table);
            self::assertSame('name', $exception->repositoryName);
            self::assertSame(RepositoryTestReadModel::class, $exception->readModelClass);
        }
    }

    private function createRepository(DynamoDbClient $client): DeferredDynamoDbRepository
    {
        return new DeferredDynamoDbRepository(
            $this->createStorage($client, RepositoryTestReadModel::class),
            new ReadModelFieldMatcher()
        );
    }

    private function createMutableRepository(DynamoDbClient $client): DeferredDynamoDbRepository
    {
        return new DeferredDynamoDbRepository(
            $this->createStorage($client, MutableRepositoryTestReadModel::class),
            new ReadModelFieldMatcher()
        );
    }

    private function createStorage(DynamoDbClient $client, string $class): DynamoDbReadModelStorage
    {
        return new DynamoDbReadModelStorage(
            $client,
            new InputBuilder(),
            new SimpleInterfaceSerializer(),
            new JsonEncoder(),
            new JsonDecoder(),
            'table',
            'name',
            $class,
            new ReadModelSnapshotStore()
        );
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

    private function queryOutputFor(RepositoryTestReadModel ...$models): QueryOutput
    {
        $output = $this->createStub(QueryOutput::class);
        $output->method('getCount')->willReturn(count($models));

        $items = array_map(function (RepositoryTestReadModel $model): array {
            return [
                'Id' => new AttributeValue([
                    'S' => $model->getId(),
                ]),
                'Data' => new AttributeValue([
                    'S' => (new JsonEncoder())->encode((new SimpleInterfaceSerializer())->serialize($model)),
                ]),
            ];
        }, $models);

        $output->method('getItems')->willReturn($items);

        return $output;
    }

    private function queryOutputForPhysicalId(string $physicalId, RepositoryTestReadModel $model): QueryOutput
    {
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

    /**
     * @return array<mixed>
     */
    private function decodePutItemPayload(PutItemInput $input): array
    {
        $data = $input->getItem()['Data']->getS();
        self::assertIsString($data);

        return json_decode($data, true, flags: JSON_THROW_ON_ERROR);
    }

    private function stringableId(string $id): object
    {
        return new class($id) {
            public function __construct(
                private readonly string $id
            ) {
            }

            public function __toString(): string
            {
                return $this->id;
            }
        };
    }
}

final class MutableRepositoryTestReadModel implements SerializableReadModel
{
    public function __construct(
        private readonly string $id,
        private string $name
    ) {
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function rename(string $name): void
    {
        $this->name = $name;
    }

    /**
     * @return array{id: string, name: string}
     */
    public function serialize(): array
    {
        return [
            'id'   => $this->id,
            'name' => $this->name,
        ];
    }

    /**
     * @param array{id: string, name: string} $data
     */
    public static function deserialize(array $data): self
    {
        return new self($data['id'], $data['name']);
    }
}
