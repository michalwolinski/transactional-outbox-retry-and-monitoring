<?php

declare(strict_types=1);

namespace Outbox;

/**
 * What one pass of the relay did, by the same counters it writes to outbox_relay.
 */
final class RelayReport
{
    public int $claimed = 0;

    /** @var array<string, int> */
    private array $counts = [];

    public function add(Counter $counter): void
    {
        $this->counts[$counter->value] = $this->count($counter) + 1;
    }

    public function count(Counter $counter): int
    {
        return $this->counts[$counter->value] ?? 0;
    }

    public function __toString(): string
    {
        return sprintf(
            'claimed %d, published %d, retried %d, dead-lettered %d, lease lost %d',
            $this->claimed,
            $this->count(Counter::Published),
            $this->count(Counter::Retried),
            $this->count(Counter::DeadLettered),
            $this->count(Counter::LeaseLost),
        );
    }
}
