<?php
declare(strict_types=1);

namespace Phper666\MongoDb\Utils\Contract;
/**
 * Interface Arrayable
 */
interface Arrayable
{
    /**
     * Get the instance as an array.
     *
     * @return array
     */
    public function toArray(): array;
}
