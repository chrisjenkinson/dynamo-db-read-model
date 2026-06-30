<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use Broadway\ReadModel\Identifiable;
use chrisjenkinson\DynamoDbReadModel\Exception\CannotFilterWithNonexistentKey;
use ReflectionClass;

final class ReadModelFieldMatcher
{
    /**
     * @param array<string, mixed> $fields
     */
    public function matches(Identifiable $model, array $fields): bool
    {
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
    }
}
