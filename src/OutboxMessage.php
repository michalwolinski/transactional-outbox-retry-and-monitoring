<?php

declare(strict_types=1);

namespace Outbox;

use Time\Duration;

/**
 * One claimed outbox row, as the relay sees it. The ages come from the database clock, so two
 * relays on two hosts with skewed clocks still agree on how old an event is.
 */
final readonly class OutboxMessage
{
    public function __construct(
        public int $id,
        public string $eventId,
        public string $aggregateId,
        public string $eventType,
        public string $payload,
        public int $attempts,
        public int $unexplainedFailures,
        public Duration $retryAge,
    ) {
    }

    /**
     * @param  array{id: int|string, event_id: string, aggregate_id: string, event_type: string, payload: string, attempts: int|string, unexplained_failures: int|string, retry_age_seconds: int|string}  $row
     */
    public static function fromRow(array $row): self
    {
        return new self(
            id: (int) $row['id'],
            eventId: $row['event_id'],
            aggregateId: $row['aggregate_id'],
            eventType: $row['event_type'],
            payload: $row['payload'],
            attempts: (int) $row['attempts'],
            unexplainedFailures: (int) $row['unexplained_failures'],
            retryAge: Duration::fromSeconds((int) $row['retry_age_seconds']),
        );
    }
}
