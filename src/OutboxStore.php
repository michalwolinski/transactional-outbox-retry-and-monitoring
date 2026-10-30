<?php

declare(strict_types=1);

namespace Outbox;

use PDO;
use Throwable;
use Time\Duration;

/**
 * Every state change of an outbox row after it was written. Each method is one short transaction;
 * none of them is open while the broker is being called.
 */
final readonly class OutboxStore
{
    /** Public so the planner test can EXPLAIN exactly what runs. */
    public const string CLAIM_SQL = <<<'SQL'
        WITH due AS (
            SELECT o.id
            FROM outbox o
            WHERE o.status = 'pending'
              AND o.next_attempt_at <= now()
              AND (o.locked_until IS NULL OR o.locked_until < now())
              -- Per-aggregate order: nothing goes out while an earlier event of the same
              -- aggregate is still pending or dead.
              AND NOT EXISTS (
                  SELECT 1
                  FROM outbox earlier
                  WHERE earlier.aggregate_id = o.aggregate_id
                    AND earlier.id < o.id
                    AND earlier.status <> 'published'
              )
            ORDER BY o.next_attempt_at, o.id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        )
        UPDATE outbox AS o
        SET attempts             = o.attempts + 1,
            -- A lease that is still set was never released: the last claimer died or ran
            -- out of time. That attempt counts, and it explains nothing.
            unexplained_failures = o.unexplained_failures
                                   + CASE WHEN o.locked_by IS NULL THEN 0 ELSE 1 END,
            locked_by            = :relay_id,
            locked_until         = now() + make_interval(secs => CAST(:lease AS integer))
        FROM due
        WHERE o.id = due.id
        RETURNING o.id, o.event_id, o.aggregate_id, o.event_type, o.payload, o.attempts,
                  o.unexplained_failures,
                  extract(epoch FROM now() - coalesce(o.retry_since, now()))::bigint AS retry_age_seconds
        SQL;

    public function __construct(private PDO $pdo)
    {
    }

    /**
     * Claim up to $limit due rows for $relayId, for the length of $lease.
     *
     * The row lock lives only inside this statement. What survives the commit is the lease
     * (locked_by, locked_until): other relays skip the row until the lease runs out, and a relay
     * that dies mid-publish gives the row back simply by not renewing anything.
     *
     * @return list<OutboxMessage> in id order
     */
    public function claim(string $relayId, int $limit, Duration $lease): array
    {
        $statement = $this->pdo->prepare(self::CLAIM_SQL);
        $statement->execute(['limit' => $limit, 'relay_id' => $relayId, 'lease' => $lease->seconds]);

        /** @var list<array{id: int|string, event_id: string, aggregate_id: string, event_type: string, payload: string, attempts: int|string, unexplained_failures: int|string, retry_age_seconds: int|string}> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
        $messages = array_map(OutboxMessage::fromRow(...), $rows);
        usort($messages, static fn (OutboxMessage $a, OutboxMessage $b): int => $a->id <=> $b->id);

        return $messages;
    }

    /**
     * @return bool false when the lease was lost: another relay owns the row now, and will
     *              publish it again
     */
    public function markPublished(OutboxMessage $message, string $relayId): bool
    {
        return $this->transition($message, $relayId, Counter::Published, <<<'SQL'
            UPDATE outbox
            SET status = 'published', published_at = now(), last_error = NULL,
                locked_by = NULL, locked_until = NULL
            WHERE id = :id AND locked_by = :relay_id AND status = 'pending'
            SQL);
    }

    public function scheduleRetry(OutboxMessage $message, string $relayId, Duration $delay, string $error, bool $unexplained): bool
    {
        return $this->transition($message, $relayId, Counter::Retried, <<<'SQL'
            UPDATE outbox
            SET next_attempt_at = now() + make_interval(secs => CAST(:delay AS integer)),
                retry_since = coalesce(retry_since, now()),
                last_error = :error,
                unexplained_failures = unexplained_failures + CAST(:unexplained AS integer),
                locked_by = NULL, locked_until = NULL
            WHERE id = :id AND locked_by = :relay_id AND status = 'pending'
            SQL, ['delay' => $delay->seconds, 'error' => $error, 'unexplained' => (int) $unexplained]);
    }

    public function deadLetter(OutboxMessage $message, string $relayId, string $reason): bool
    {
        return $this->transition($message, $relayId, Counter::DeadLettered, <<<'SQL'
            UPDATE outbox
            SET status = 'dead', dead_at = now(), last_error = :error,
                locked_by = NULL, locked_until = NULL
            WHERE id = :id AND locked_by = :relay_id AND status = 'pending'
            SQL, ['error' => $reason]);
    }

    /**
     * Put a dead event back. Its retry window and its unexplained failures start over, and its
     * aggregate's held events follow it out in order. last_error stays, as history.
     */
    public function requeue(string $eventId): bool
    {
        $statement = $this->pdo->prepare(<<<'SQL'
            UPDATE outbox
            SET status = 'pending', dead_at = NULL, attempts = 0, unexplained_failures = 0,
                retry_since = NULL, next_attempt_at = now()
            WHERE event_id = :event_id AND status = 'dead'
            SQL);
        $statement->execute(['event_id' => $eventId]);

        return 1 === $statement->rowCount();
    }

    /**
     * @return list<array{event_id: string, aggregate_id: string, event_type: string, attempts: int, last_error: string|null, dead_at: string, held: int}>
     */
    public function deadLetters(): array
    {
        $rows = $this->pdo->query(<<<'SQL'
            SELECT d.event_id, d.aggregate_id, d.event_type, d.attempts, d.last_error, d.dead_at,
                   (SELECT count(*)
                    FROM outbox later
                    WHERE later.aggregate_id = d.aggregate_id
                      AND later.id > d.id
                      AND later.status = 'pending') AS held
            FROM outbox d
            WHERE d.status = 'dead'
            ORDER BY d.id
            SQL)->fetchAll(PDO::FETCH_ASSOC);

        return array_map(static fn (array $row): array => [
            'event_id' => (string) $row['event_id'],
            'aggregate_id' => (string) $row['aggregate_id'],
            'event_type' => (string) $row['event_type'],
            'attempts' => (int) $row['attempts'],
            'last_error' => null === $row['last_error'] ? null : (string) $row['last_error'],
            'dead_at' => (string) $row['dead_at'],
            'held' => (int) $row['held'],
        ], $rows);
    }

    /**
     * Extend the lease on every row of the batch this relay still holds, and beat. Called before
     * each publish, so the lease has to outlast one publish rather than the whole batch, and the
     * heartbeat keeps going through a long pass.
     *
     * @param  list<int>  $ids
     * @return list<int> the ids still held; a row missing here was taken over, and publishing it
     *                   would only be a duplicate
     */
    public function renew(string $relayId, array $ids, Duration $lease): array
    {
        $this->beat($relayId);

        $statement = $this->pdo->prepare(<<<'SQL'
            UPDATE outbox
            SET locked_until = now() + make_interval(secs => CAST(:lease AS integer))
            WHERE id = ANY(CAST(:ids AS bigint[])) AND locked_by = :relay_id AND status = 'pending'
            RETURNING id
            SQL);
        $statement->execute(['ids' => '{'.implode(',', $ids).'}', 'relay_id' => $relayId, 'lease' => $lease->seconds]);

        return array_map(intval(...), $statement->fetchAll(PDO::FETCH_COLUMN));
    }

    /**
     * Give back rows this relay claimed and never started: it is shutting down, not dying. The
     * attempt the claim counted is taken back too, and no lease is left behind, so the next claim
     * does not record an unexplained failure against an event nobody tried.
     *
     * @param  list<int>  $ids
     */
    public function release(string $relayId, array $ids): int
    {
        $statement = $this->pdo->prepare(<<<'SQL'
            UPDATE outbox
            SET locked_by = NULL, locked_until = NULL, attempts = attempts - 1
            WHERE id = ANY(CAST(:ids AS bigint[])) AND locked_by = :relay_id AND status = 'pending'
            SQL);
        $statement->execute(['ids' => '{'.implode(',', $ids).'}', 'relay_id' => $relayId]);

        return $statement->rowCount();
    }

    public function beat(string $relayId): void
    {
        $this->pdo->prepare(<<<'SQL'
            INSERT INTO outbox_relay (relay_id) VALUES (:relay_id)
            ON CONFLICT (relay_id) DO UPDATE SET beat_at = now()
            SQL)->execute(['relay_id' => $relayId]);
    }

    /**
     * Move a claimed row and count the move, in one transaction. Zero rows means the lease is
     * gone and another relay has the row; that is counted too. A late success means the event
     * went out twice; a late failure means it may have.
     *
     * @param  array<string, int|string>  $parameters
     */
    private function transition(OutboxMessage $message, string $relayId, Counter $counter, string $sql, array $parameters = []): bool
    {
        $this->pdo->beginTransaction();

        try {
            $statement = $this->pdo->prepare($sql);
            $statement->execute(['id' => $message->id, 'relay_id' => $relayId, ...$parameters]);
            $moved = 1 === $statement->rowCount();

            // An upsert, so a relay that has not beaten yet still has its verdict counted.
            $column = $moved ? $counter->value : Counter::LeaseLost->value;
            $this->pdo->prepare(<<<SQL
                INSERT INTO outbox_relay (relay_id, {$column}) VALUES (:relay_id, 1)
                ON CONFLICT (relay_id) DO UPDATE SET {$column} = outbox_relay.{$column} + 1
                SQL)->execute(['relay_id' => $relayId]);

            $this->pdo->commit();
        } catch (Throwable $exception) {
            $this->pdo->rollBack();

            throw $exception;
        }

        return $moved;
    }
}
