<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\Core\Response;
use AsyncAws\Core\Test\Http\SimpleMockedResponse;
use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\Result\DeleteItemOutput;
use AsyncAws\DynamoDb\Result\PutItemOutput;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\SimpleInterfaceSerializer;
use chrisjenkinson\DynamoDbReadModel\DynamoDbRepository;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use chrisjenkinson\DynamoDbReadModel\JsonDecoder;
use chrisjenkinson\DynamoDbReadModel\JsonEncoder;
use chrisjenkinson\DynamoDbReadModel\ReadModelSnapshotStore;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\MockHttpClient;

final class DynamoDbRepositoryResponseResolutionTest extends TestCase
{
    /**
     * @test
     */
    public function it_resolves_the_put_item_response_when_saving(): void
    {
        $output = new PutItemOutput($this->createResponse());
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('putItem')->willReturn($output);

        $this->createRepository($client)->save(new RepositoryTestReadModel('id', 'name', 'foo', []));

        $this->assertTrue($output->info()['resolved']);
    }

    /**
     * @test
     */
    public function it_resolves_the_delete_item_response_when_removing(): void
    {
        $output = new DeleteItemOutput($this->createResponse());
        $client = $this->createMock(DynamoDbClient::class);
        $client->expects($this->once())->method('deleteItem')->willReturn($output);

        $this->createRepository($client)->remove('id');

        $this->assertTrue($output->info()['resolved']);
    }

    private function createRepository(DynamoDbClient $client): DynamoDbRepository
    {
        return new DynamoDbRepository(
            $client,
            new InputBuilder(),
            new SimpleInterfaceSerializer(),
            new JsonEncoder(),
            new JsonDecoder(),
            'table',
            'name',
            RepositoryTestReadModel::class,
            new ReadModelSnapshotStore()
        );
    }

    private function createResponse(): Response
    {
        $client = new MockHttpClient(new SimpleMockedResponse('{}'));

        return new Response($client->request('POST', 'http://localhost'), $client, new NullLogger());
    }
}
