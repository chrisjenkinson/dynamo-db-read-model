<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel\Tests;

use chrisjenkinson\DynamoDbReadModel\JsonDecoder;
use JsonException;
use PHPUnit\Framework\TestCase;

final class JsonDecoderTest extends TestCase
{
    public function test_it_decodes_json_objects_as_arrays(): void
    {
        self::assertSame([
            'key' => 'value',
        ], (new JsonDecoder())->decode('{"key":"value"}'));
    }

    public function test_it_rejects_json_that_does_not_decode_to_an_array(): void
    {
        $this->expectException(JsonException::class);
        $this->expectExceptionMessage('Decoded JSON did not produce an array.');

        (new JsonDecoder())->decode('"value"');
    }
}
