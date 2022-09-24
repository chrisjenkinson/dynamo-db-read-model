<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use AsyncAws\DynamoDb\DynamoDbClient;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Repository;
use Broadway\Serializer\Serializer;
use ReflectionClass;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedEncodedData;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use chrisjenkinson\DynamoDbReadModel\Exception\CannotFilterWithNonexistentKey;

final class DynamoDbRepository implements Repository
{
    public function __construct(
        private readonly DynamoDbClient $client,
        private readonly InputBuilder $inputBuilder,
        private readonly Serializer $serializer,
        private readonly JsonEncoder $jsonEncoder,
        private readonly JsonDecoder $jsonDecoder,
        private readonly string $table,
        private readonly string $name,
        private readonly string $class
    ) {
    }

    public function save(Identifiable $data): void
    {
        $encodedData = $this->jsonEncoder->encode($this->serializer->serialize($data));

        $putItemInput = $this->inputBuilder->buildPutItemInput($this->table, $this->name, $data->getId(), $encodedData);

        $this->client->putItem($putItemInput);
    }

    public function find($id): ?Identifiable
    {
        $result = $this->client->getItem($this->inputBuilder->buildGetItemInput($this->table, $this->name, (string) $id));

        $item = $result->getItem();

        if ([] === $item) {
            return null;
        }

        $encodedData = $item['Data']->getS();

        if (!is_string($encodedData)) {
            throw new UnexpectedEncodedData('Data', 'string', $encodedData);
        }

        $data = $this->serializer->deserialize($this->jsonDecoder->decode($encodedData));

        if (!($data instanceof $this->class && $data instanceof Identifiable)) {
            throw new UnexpectedReadModel(actual: $data::class, expected: $this->class);
        }

        return $data;
    }

    /**
     * @param array<string, mixed> $fields
     */
    public function findBy(array $fields): array
    {
        if ([] === $fields) {
            return [];
        }

        $result = $this->client->query($this->inputBuilder->buildQueryInput($this->table, $this->name));

        if (0 === $result->getCount()) {
            return [];
        }

        $items = [];

        foreach ($result->getItems() as $item) {
            $encodedData = $item['Data']->getS();

            if (!is_string($encodedData)) {
                throw new UnexpectedEncodedData('Data', 'string', $encodedData);
            }

            $data = $this->serializer->deserialize($this->jsonDecoder->decode($encodedData));

            if (!($data instanceof $this->class && $data instanceof Identifiable)) {
                throw new UnexpectedReadModel(actual: $data::class, expected: $this->class);
            }

            $items[] = $data;
        }

        $items = array_filter($items, function (Identifiable $model) use ($fields): bool {
            $reflectedClass = new ReflectionClass($model);

            foreach ($fields as $field => $value) {
                $methodName = sprintf('get%s', ucfirst($field));

                if ($reflectedClass->hasMethod($methodName)) {
                    $method = $reflectedClass->getMethod($methodName);
                    if (!$method->isPublic()) {
                        throw new CannotFilterWithNonexistentKey(model: $model::class, key: $field);
                    }
                    $modelValue = $method->invoke($model);
                } elseif ($reflectedClass->hasProperty($field)) {
                    $property = $reflectedClass->getProperty($field);
                    if (!$property->isPublic()) {
                        throw new CannotFilterWithNonexistentKey(model: $model::class, key: $field);
                    }
                    $modelValue = $property->getValue($model);
                } else {
                    throw new CannotFilterWithNonexistentKey(model: $model::class, key: $field);
                }

                if (is_array($modelValue) && !in_array($value, $modelValue, true)) {
                    return false;
                }

                if (!is_array($modelValue) && $modelValue !== $value) {
                    return false;
                }
            }

            return true;
        });

        return array_values($items);
    }

    public function findAll(): array
    {
        $result = $this->client->query($this->inputBuilder->buildQueryInput($this->table, $this->name));

        if (0 === $result->getCount()) {
            return [];
        }

        $items = [];

        foreach ($result->getItems() as $item) {
            $encodedData = $item['Data']->getS();

            if (!is_string($encodedData)) {
                throw new UnexpectedEncodedData('Data', 'string', $encodedData);
            }

            $data = $this->serializer->deserialize($this->jsonDecoder->decode($encodedData));

            if (!($data instanceof $this->class && $data instanceof Identifiable)) {
                throw new UnexpectedReadModel(actual: $data::class, expected: $this->class);
            }

            $items[] = $data;
        }

        return $items;
    }

    public function remove($id): void
    {
        $this->client->deleteItem($this->inputBuilder->buildDeleteItemInput($this->table, $this->name, (string) $id));
    }
}
