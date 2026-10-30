<?php

declare(strict_types=1);

namespace Outbox\Tests;

use Outbox\Broker\TransientPublishFailure;
use Outbox\Counter;
use Outbox\Monitoring\OutboxHealth;
use Outbox\OutboxStore;
use Outbox\Tests\Support\OutboxTestCase;
use PHPUnit\Framework\Attributes\Test;

final class HealthTest extends OutboxTestCase
{
    #[Test]
    public function an_empty_outbox_reads_all_zeros(): void
    {
        $health = $this->health()->snapshot();

        $this->assertSame(0, $health->pending);
        $this->assertSame(0.0, $health->oldestPendingAgeSeconds);
        $this->assertSame(0, $health->relaysAlive);
        $this->assertSame(0, $health->total(Counter::Published));
    }

    #[Test]
    public function lag_is_the_age_of_the_oldest_event_and_a_backlog_count_cannot_see_it(): void
    {
        // One event stuck for ten minutes: a backlog of one, which looks perfectly healthy.
        $this->append('order-1');
        $this->wait(600);
        $stuck = $this->health()->snapshot();

        $this->assertSame(1, $stuck->pending);
        $this->assertGreaterThanOrEqual(600.0, $stuck->oldestPendingAgeSeconds);
        $this->assertLessThan(601.0, $stuck->oldestPendingAgeSeconds);

        // A thousand events written just now, next to it: the backlog jumps, the lag does not move.
        $this->pdo->exec(<<<'SQL'
            INSERT INTO outbox (aggregate_id, event_type, payload)
            SELECT 'order-' || n, 'order.placed', '{}' FROM generate_series(2, 1001) n
            SQL);
        $burst = $this->health()->snapshot();

        $this->assertSame(1001, $burst->pending);
        $this->assertGreaterThanOrEqual(600.0, $burst->oldestPendingAgeSeconds);

        // The stuck one goes out: a backlog of a thousand that looks alarming, and a lag of zero.
        $this->pdo->exec("UPDATE outbox SET status = 'published' WHERE aggregate_id = 'order-1'");
        $healthy = $this->health()->snapshot();

        $this->assertSame(1000, $healthy->pending);
        $this->assertLessThan(1.0, $healthy->oldestPendingAgeSeconds);
    }

    #[Test]
    public function retrying_counts_pending_events_that_have_failed_at_least_once(): void
    {
        $failing = $this->append('order-1');
        $this->append('order-2');
        $this->broker->script($failing, new TransientPublishFailure('timeout'));
        $this->append('order-3');
        $this->pdo->exec("UPDATE outbox SET next_attempt_at = now() + interval '1 hour' WHERE aggregate_id = 'order-3'");

        $this->relay()->runOnce();
        $health = $this->health()->snapshot();

        $this->assertSame(2, $health->pending);
        $this->assertSame(1, $health->retrying);
    }

    #[Test]
    public function a_relay_is_alive_only_while_its_heartbeat_is_recent(): void
    {
        $this->store->beat('relay-old');
        $this->wait(31);
        $this->store->beat('relay-new');

        $this->assertSame(1, $this->health()->snapshot()->relaysAlive);
    }

    #[Test]
    public function counters_are_summed_across_relays_and_survive_a_restart(): void
    {
        $this->append('order-1');
        $this->relay('relay-a')->runOnce();
        $this->append('order-2');
        $this->append('order-3');
        $this->relay('relay-b')->runOnce();

        // A restarted relay is a new PHP process; the counters live in the table, not in it.
        $restarted = new OutboxHealth($this->pdo)->snapshot();

        $this->assertSame(3, $restarted->total(Counter::Published));
    }

    #[Test]
    public function neither_the_claim_nor_the_health_query_reads_the_published_history(): void
    {
        $this->pdo->exec(<<<'SQL'
            INSERT INTO outbox (aggregate_id, event_type, payload, status, published_at)
            SELECT 'order-' || (n % 5000), 'order.placed', '{}', 'published', now()
            FROM generate_series(1, 200000) n
            SQL);
        $this->append('order-1');
        $this->append('order-2');
        $this->pdo->exec('ANALYZE outbox');

        $claim = $this->plan(OutboxStore::CLAIM_SQL, ['limit' => 100, 'relay_id' => 'relay-a', 'lease' => 30]);
        $health = $this->plan(OutboxHealth::OUTBOX_SQL);

        $this->assertNotContains('Seq Scan', $claim);
        $this->assertNotContains('Seq Scan', $health);
    }

    /**
     * @param  array<string, int|string>  $parameters
     * @return list<string> every node type in the plan
     */
    private function plan(string $sql, array $parameters = []): array
    {
        $statement = $this->pdo->prepare('EXPLAIN (FORMAT JSON) '.$sql);
        $statement->execute($parameters);
        $plan = json_decode((string) $statement->fetchColumn(), true, flags: JSON_THROW_ON_ERROR);

        $types = [];
        array_walk_recursive($plan, static function (mixed $value, string|int $key) use (&$types): void {
            if ('Node Type' === $key) {
                $types[] = (string) $value;
            }
        });

        return $types;
    }
}
