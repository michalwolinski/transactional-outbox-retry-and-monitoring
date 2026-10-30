<?php

declare(strict_types=1);

namespace Outbox\Monitoring;

use Outbox\Counter;

final readonly class HealthSnapshot
{
    /**
     * @param  array<value-of<Counter>, int>  $totals
     */
    public function __construct(
        public int $pending,
        public int $retrying,
        public int $dead,
        public int $held,
        public int $parkedAggregates,
        public float $oldestPendingAgeSeconds,
        public int $relaysAlive,
        public array $totals,
    ) {
    }

    public function total(Counter $counter): int
    {
        return $this->totals[$counter->value];
    }
}
