<?php

declare(strict_types=1);

namespace Outbox\Tests\Support;

use Outbox\Broker\ScriptedBroker;
use Outbox\Database;
use Outbox\Monitoring\OutboxHealth;
use Outbox\OutboxStore;
use Outbox\OutboxWriter;
use Outbox\Relay;
use Outbox\Retry\RetryPolicy;
use PDO;
use PHPUnit\Framework\TestCase;
use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;
use Time\Duration;

abstract class OutboxTestCase extends TestCase
{
    protected PDO $pdo;

    protected OutboxStore $store;

    protected ScriptedBroker $broker;

    protected function setUp(): void
    {
        $this->pdo = Database::connect();
        Database::reset($this->pdo);
        $this->store = new OutboxStore($this->pdo);
        $this->broker = new ScriptedBroker();
    }

    protected function relay(string $relayId = 'relay-a', ?RetryPolicy $policy = null): Relay
    {
        return new Relay($this->store, $this->broker, $policy ?? $this->policy(), $relayId, self::lease());
    }

    protected function policy(int $maxUnexplainedFailures = 3, ?Duration $retryWindow = null): RetryPolicy
    {
        return new RetryPolicy(
            new Randomizer(new Xoshiro256StarStar(2026)),
            retryWindow: $retryWindow,
            maxUnexplainedFailures: $maxUnexplainedFailures,
        );
    }

    /**
     * The lease every test relay takes: 30 s, which is why the tests wait 31 to see it expire.
     */
    protected static function lease(): Duration
    {
        return Duration::fromSeconds(30);
    }

    /**
     * Writes one event the way business code does: inside a transaction.
     */
    protected function append(string $aggregateId, string $eventType = 'order.placed'): string
    {
        $this->pdo->beginTransaction();
        $eventId = new OutboxWriter($this->pdo)->append($aggregateId, $eventType, ['aggregate' => $aggregateId]);
        $this->pdo->commit();

        return $eventId;
    }

    /**
     * Moves every timestamp in both tables $seconds into the past, which is the same as waiting
     * $seconds: the code under test only ever compares them with the database's now().
     */
    protected function wait(int $seconds): void
    {
        $interval = "interval '{$seconds} seconds'";
        $this->pdo->exec(<<<SQL
            UPDATE outbox SET
                created_at = created_at - {$interval},
                retry_since = retry_since - {$interval},
                next_attempt_at = next_attempt_at - {$interval},
                locked_until = locked_until - {$interval},
                published_at = published_at - {$interval},
                dead_at = dead_at - {$interval}
            SQL);
        $this->pdo->exec("UPDATE outbox_relay SET beat_at = beat_at - {$interval}");
    }

    /**
     * @return array<string, mixed>
     */
    protected function row(string $eventId): array
    {
        $statement = $this->pdo->prepare('SELECT * FROM outbox WHERE event_id = :event_id');
        $statement->execute(['event_id' => $eventId]);

        /** @var array<string, mixed> */
        return $statement->fetch();
    }

    protected function statusOf(string $eventId): string
    {
        return (string) $this->row($eventId)['status'];
    }

    protected function health(): OutboxHealth
    {
        return new OutboxHealth($this->pdo);
    }
}
