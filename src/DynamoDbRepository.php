<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use AsyncAws\DynamoDb\DynamoDbClient;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Repository;
use Broadway\Serializer\Serializer;
use chrisjenkinson\DynamoDbReadModel\Exception\CannotFilterWithNonexistentKey;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedEncodedData;
use chrisjenkinson\DynamoDbReadModel\Exception\UnexpectedReadModel;
use ReflectionClass;

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

        return $this->deserializeReadModel($encodedData);
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

            $items[] = $this->deserializeReadModel($encodedData);
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

            $items[] = $this->deserializeReadModel($encodedData);
        }

        return $items;
    }

    public function remove($id): void
    {
        $this->client->deleteItem($this->inputBuilder->buildDeleteItemInput($this->table, $this->name, (string) $id));
    }

    private function deserializeReadModel(string $encodedData): Identifiable
    {
        $serializedData = $this->jsonDecoder->decode($encodedData);

        if (!isset($serializedData['class']) || !is_string($serializedData['class'])) {
            throw new UnexpectedReadModel(actual: $this->describeSerializedClass($serializedData['class'] ?? null), expected: $this->class);
        }

        if ($serializedData['class'] !== $this->class) {
            throw new UnexpectedReadModel(actual: $serializedData['class'], expected: $this->class);
        }

        $data = $this->serializer->deserialize($serializedData);

        if (!($data instanceof $this->class && $data instanceof Identifiable)) {
            throw new UnexpectedReadModel(actual: $data::class, expected: $this->class);
        }

        return $data;
    }

    private function describeSerializedClass(mixed $class): string
    {
        if (is_string($class)) {
            return $class;
        }

        if (is_scalar($class) || null === $class) {
            return var_export($class, true);
        }

        return get_debug_type($class);
    }
}
