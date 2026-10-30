<?php

declare(strict_types=1);

namespace Outbox;

use LogicException;
use PDO;

/**
 * The write side: one INSERT in the caller's transaction. Nothing about retry lives here — the
 * business code does not know the relay exists.
 */
final readonly class OutboxWriter
{
    public function __construct(private PDO $pdo)
    {
    }

    /**
     * @param  array<string, mixed>  $payload
     * @return string the event id, which travels with the message so consumers can deduplicate
     */
    public function append(string $aggregateId, string $eventType, array $payload): string
    {
        // Outside a transaction this INSERT commits on its own, and the business write next to it
        // commits separately: that is the dual write the outbox exists to remove.
        if (false === $this->pdo->inTransaction()) {
            throw new LogicException('Outbox writes belong in the same transaction as the business write.');
        }

        // Ids are handed out at INSERT and become visible at COMMIT, and those two orders differ
        // between concurrent transactions. Without this lock a slow writer's "placed" (id 10) can
        // still be uncommitted when a fast writer's "paid" (id 11) is claimed: the relay cannot
        // see 10, so 11 goes out first. Writers of one aggregate queue here until the earlier one
        // commits; writers of different aggregates almost never meet (hashtext can collide).
        // The two-key form keeps these locks in an 'outbox' namespace, apart from any other
        // advisory lock the application takes. A transaction appending for several aggregates
        // should do it in a fixed order, or two of them can deadlock (PostgreSQL aborts one).
        $this->pdo->prepare("SELECT pg_advisory_xact_lock(hashtext('outbox'), hashtext(:aggregate_id))")
            ->execute(['aggregate_id' => $aggregateId]);

        $statement = $this->pdo->prepare(
            'INSERT INTO outbox (aggregate_id, event_type, payload)
             VALUES (:aggregate_id, :event_type, :payload)
             RETURNING event_id',
        );
        $statement->execute([
            'aggregate_id' => $aggregateId,
            'event_type' => $eventType,
            'payload' => json_encode($payload, JSON_THROW_ON_ERROR),
        ]);

        return (string) $statement->fetchColumn();
    }
}
