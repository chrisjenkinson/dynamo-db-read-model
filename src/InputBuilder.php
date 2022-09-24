<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use AsyncAws\DynamoDb\Input\CreateTableInput;
use AsyncAws\DynamoDb\Input\DeleteItemInput;
use AsyncAws\DynamoDb\Input\DeleteTableInput;
use AsyncAws\DynamoDb\Input\DescribeTableInput;
use AsyncAws\DynamoDb\Input\GetItemInput;
use AsyncAws\DynamoDb\Input\PutItemInput;
use AsyncAws\DynamoDb\Input\QueryInput;
use AsyncAws\DynamoDb\ValueObject\AttributeDefinition;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use AsyncAws\DynamoDb\ValueObject\KeySchemaElement;

final class InputBuilder
{
    public function buildDescribeTableInput(string $tableName): DescribeTableInput
    {
        return new DescribeTableInput([
            'TableName' => $tableName,
        ]);
    }

    public function buildDeleteTableInput(string $tableName): DeleteTableInput
    {
        return new DeleteTableInput([
            'TableName' => $tableName,
        ]);
    }

    public function buildCreateTableInput(string $tableName): CreateTableInput
    {
        return new CreateTableInput([
            'TableName'            => $tableName,
            'AttributeDefinitions' => [
                new AttributeDefinition([
                    'AttributeName' => 'Name',
                    'AttributeType' => 'S',
                ]),
                new AttributeDefinition([
                    'AttributeName' => 'Id',
                    'AttributeType' => 'S',
                ]),
            ],
            'BillingMode' => 'PAY_PER_REQUEST',
            'KeySchema'   => [
                new KeySchemaElement([
                    'AttributeName' => 'Name',
                    'KeyType'       => 'HASH',
                ]),
                new KeySchemaElement([
                    'AttributeName' => 'Id',
                    'KeyType'       => 'RANGE',
                ]),
            ],
        ]);
    }

    public function buildPutItemInput(string $table, string $name, string $id, string $encodedData): PutItemInput
    {
        return new PutItemInput([
            'TableName' => $table,
            'Item'      => [
                'Name' => new AttributeValue([
                    'S' => $name,
                ]),
                'Id' => new AttributeValue([
                    'S' => $id,
                ]),
                'Data' => new AttributeValue([
                    'S' => $encodedData,
                ]),
            ],
        ]);
    }

    public function buildGetItemInput(string $table, string $name, string $id): GetItemInput
    {
        return new GetItemInput([
            'TableName' => $table,
            'Key'       => [
                'Name' => new AttributeValue([
                    'S' => $name,
                ]),
                'Id' => new AttributeValue([
                    'S' => $id,
                ]),
            ],
            'ProjectionExpression'     => '#Data',
            'ExpressionAttributeNames' => [
                '#Data' => 'Data',
            ],
        ]);
    }

    public function buildQueryInput(string $table, string $name): QueryInput
    {
        return new QueryInput([
            'TableName'                => $table,
            'KeyConditionExpression'   => '#Name = :name',
            'ExpressionAttributeNames' => [
                '#Name' => 'Name',
            ],
            'ExpressionAttributeValues' => [
                ':name' => new AttributeValue([
                    'S' => $name,
                ]),
            ],
        ]);
    }

    public function buildDeleteItemInput(string $table, string $name, string $id): DeleteItemInput
    {
        return new DeleteItemInput([
            'TableName' => $table,
            'Key'       => [
                'Name' => new AttributeValue([
                    'S' => $name,
                ]),
                'Id' => new AttributeValue([
                    'S' => $id,
                ]),
            ],
        ]);
    }
}
