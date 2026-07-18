<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use AsyncAws\DynamoDb\Input\BatchGetItemInput;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use AsyncAws\DynamoDb\ValueObject\KeysAndAttributes;
use chrisjenkinson\DynamoDbReadModel\InputBuilder;
use PHPUnit\Framework\TestCase;

final class InputBuilderBatchGetItemTest extends TestCase
{
    public function testBuildsBatchGetItemInputForIds(): void
    {
        $input = (new InputBuilder())->buildBatchGetItemInput('table', 'repository', ['one', 'two']);

        self::assertInstanceOf(BatchGetItemInput::class, $input);
        self::assertSame(['table'], array_keys($input->getRequestItems()));

        $keysAndAttributes = $input->getRequestItems()['table'];
        $keys              = $keysAndAttributes->getKeys();

        self::assertCount(2, $keys);
        self::assertSame(['Name', 'Id'], array_keys($keys[0]));
        self::assertSame('repository', $keys[0]['Name']->getS());
        self::assertSame('one', $keys[0]['Id']->getS());
        self::assertSame(['Name', 'Id'], array_keys($keys[1]));
        self::assertSame('repository', $keys[1]['Name']->getS());
        self::assertSame('two', $keys[1]['Id']->getS());
        self::assertSame('#Id, #Data', $keysAndAttributes->getProjectionExpression());
        self::assertSame([
            '#Id'   => 'Id',
            '#Data' => 'Data',
        ], $keysAndAttributes->getExpressionAttributeNames());
        self::assertNull($keysAndAttributes->getConsistentRead());
    }

    public function testBuildsRetryInputWithSuppliedKeysAndAttributes(): void
    {
        $keysAndAttributes = new KeysAndAttributes([
            'Keys' => [
                [
                    'Name' => new AttributeValue([
                        'S' => 'repository',
                    ]),
                    'Id' => new AttributeValue([
                        'S' => 'one',
                    ]),
                ],
            ],
        ]);

        $input = (new InputBuilder())->buildBatchGetItemRetryInput('table', $keysAndAttributes);

        self::assertInstanceOf(BatchGetItemInput::class, $input);
        self::assertSame(['table'], array_keys($input->getRequestItems()));
        self::assertSame($keysAndAttributes, $input->getRequestItems()['table']);
    }
}
