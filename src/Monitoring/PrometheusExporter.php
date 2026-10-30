<?php

declare(strict_types=1);

namespace Outbox\Monitoring;

use Outbox\Counter;

/**
 * Prometheus text exposition format, version 0.0.4. No labels on purpose: event_type or
 * aggregate_id as a label is a cardinality bill, and the dead-letter list has the details.
 */
final readonly class PrometheusExporter
{
    private const array COUNTER_HELP = [
        'published' => 'Events the broker confirmed.',
        'retried' => 'Failed attempts that were scheduled for another try.',
        'dead_lettered' => 'Events moved to dead: permanent failure, exhausted window, or too many unexplained failures.',
        'lease_lost' => 'Verdicts that arrived after the lease had passed to another relay: a publish slower than the lease, and a likely duplicate.',
    ];

    public function render(HealthSnapshot $health): string
    {
        $lines = [];
        $gauge = static function (string $name, string $help, int|float $value) use (&$lines): void {
            $lines[] = "# HELP {$name} {$help}";
            $lines[] = "# TYPE {$name} gauge";
            $lines[] = "{$name} {$value}";
        };

        $gauge('outbox_oldest_pending_age_seconds', 'Age of the oldest event not yet published. The lag a consumer sees.', $health->oldestPendingAgeSeconds);
        $gauge('outbox_pending_rows', 'Events waiting to be published, including those in backoff.', $health->pending);
        $gauge('outbox_retrying_rows', 'Pending events that have failed at least once.', $health->retrying);
        $gauge('outbox_dead_rows', 'Events the relay gave up on. Each one needs a human.', $health->dead);
        $gauge('outbox_held_rows', 'Pending events stuck behind a dead event of the same aggregate.', $health->held);
        $gauge('outbox_parked_aggregates', 'Aggregates with a dead event; nothing more of theirs goes out.', $health->parkedAggregates);
        $gauge('outbox_relays_alive', 'Relays that sent a heartbeat recently.', $health->relaysAlive);

        foreach (Counter::cases() as $counter) {
            $name = "outbox_{$counter->value}_total";
            $lines[] = "# HELP {$name} ".self::COUNTER_HELP[$counter->value];
            $lines[] = "# TYPE {$name} counter";
            $lines[] = "{$name} {$health->total($counter)}";
        }

        return implode("\n", $lines)."\n";
    }
}
