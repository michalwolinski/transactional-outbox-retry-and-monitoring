<?php

declare(strict_types=1);

namespace Outbox\Monitoring;

use Outbox\Counter;
use PDO;
use Time\Duration;

/**
 * The outbox's health, read from the tables at scrape time. Nothing here is kept in a process,
 * so the numbers survive relay restarts and do not depend on which relay you ask.
 */
final readonly class OutboxHealth
{
    /** One pass over the unpublished rows; public so the planner test can EXPLAIN it. */
    public const string OUTBOX_SQL = <<<'SQL'
        SELECT
            count(*) FILTER (WHERE o.status = 'pending')                          AS pending,
            count(*) FILTER (WHERE o.status = 'pending' AND o.retry_since IS NOT NULL) AS retrying,
            count(*) FILTER (WHERE o.status = 'dead')                             AS dead,
            -- Pending rows that cannot move until a human acts: an earlier event of their
            -- aggregate is dead.
            count(*) FILTER (WHERE o.status = 'pending' AND EXISTS (
                SELECT 1 FROM outbox d
                WHERE d.aggregate_id = o.aggregate_id AND d.id < o.id AND d.status = 'dead'
            ))                                                                    AS held,
            count(DISTINCT o.aggregate_id) FILTER (WHERE o.status = 'dead')       AS parked_aggregates,
            coalesce(extract(epoch FROM now() - min(o.created_at) FILTER (WHERE o.status = 'pending')), 0)
                                                                                  AS oldest_pending_age_seconds
        FROM outbox o
        WHERE o.status <> 'published'
        SQL;

    public function __construct(
        private PDO $pdo,
        private ?Duration $aliveWithin = null,
    ) {
    }

    public function snapshot(): HealthSnapshot
    {
        /** @var array{pending: int, retrying: int, dead: int, held: int, parked_aggregates: int, oldest_pending_age_seconds: string} $outbox */
        $outbox = $this->pdo->query(self::OUTBOX_SQL)->fetch(PDO::FETCH_ASSOC);

        $statement = $this->pdo->prepare(<<<'SQL'
            SELECT
                count(*) FILTER (WHERE beat_at > now() - make_interval(secs => CAST(:alive AS integer))) AS alive,
                coalesce(sum(published), 0)     AS published,
                coalesce(sum(retried), 0)       AS retried,
                coalesce(sum(dead_lettered), 0) AS dead_lettered,
                coalesce(sum(lease_lost), 0)    AS lease_lost
            FROM outbox_relay
            SQL);
        $statement->execute(['alive' => ($this->aliveWithin ?? Duration::fromSeconds(30))->seconds]);
        /** @var array<string, int|string> $relays */
        $relays = $statement->fetch(PDO::FETCH_ASSOC);

        $totals = [];

        foreach (Counter::cases() as $counter) {
            $totals[$counter->value] = (int) $relays[$counter->value];
        }

        return new HealthSnapshot(
            pending: (int) $outbox['pending'],
            retrying: (int) $outbox['retrying'],
            dead: (int) $outbox['dead'],
            held: (int) $outbox['held'],
            parkedAggregates: (int) $outbox['parked_aggregates'],
            oldestPendingAgeSeconds: round((float) $outbox['oldest_pending_age_seconds'], 3),
            relaysAlive: (int) $relays['alive'],
            totals: $totals,
        );
    }
}
