<?php

declare(strict_types=1);

namespace chrisjenkinson\DynamoDbReadModel;

use chrisjenkinson\DynamoDbReadModel\Exception\InvalidIdCriterion;

final class FindByCriteria
{
    /**
     * @param list<string>        $ids
     * @param array<string,mixed> $remainingFields
     */
    private function __construct(
        public readonly bool $hasId,
        public readonly bool $multiple,
        public readonly bool $impossible,
        public readonly array $ids,
        public readonly array $remainingFields
    ) {
    }

    /**
     * @param array<string,mixed> $fields
     */
    public static function from(array $fields): self
    {
        if (!array_key_exists('id', $fields)) {
            return new self(false, false, false, [], $fields);
        }

        $criterion       = $fields['id'];
        $remainingFields = $fields;
        unset($remainingFields['id']);

        if (is_string($criterion) && '' !== $criterion) {
            return new self(true, false, false, [$criterion], $remainingFields);
        }

        if (is_array($criterion)) {
            if (!array_is_list($criterion)) {
                throw InvalidIdCriterion::nonList();
            }

            $ids     = [];
            $seenIds = [];

            foreach ($criterion as $index => $id) {
                if (!is_string($id) || '' === $id) {
                    throw InvalidIdCriterion::invalidEntry($index, $id);
                }

                if (isset($seenIds[$id])) {
                    continue;
                }

                $seenIds[$id] = true;
                $ids[]        = $id;
            }

            return new self(true, true, false, $ids, $remainingFields);
        }

        return new self(true, false, true, [], $remainingFields);
    }
}
