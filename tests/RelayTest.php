<?php

declare(strict_types=1);

namespace Outbox\Tests;

use Outbox\Broker\PermanentPublishFailure;
use Outbox\Broker\TransientPublishFailure;
use Outbox\Counter;
use Outbox\Tests\Support\OutboxTestCase;
use PHPUnit\Framework\Attributes\Test;
use RuntimeException;
use Time\Duration;

final class RelayTest extends OutboxTestCase
{
    #[Test]
    public function a_confirmed_publish_marks_the_row_and_counts_it(): void
    {
        $eventId = $this->append('order-1');

        $report = $this->relay()->runOnce();

        $this->assertSame(1, $report->count(Counter::Published));
        $this->assertSame('published', $this->statusOf($eventId));
        $this->assertSame(1, $this->health()->snapshot()->total(Counter::Published));
    }

    #[Test]
    public function a_relay_that_dies_between_publish_and_mark_causes_a_duplicate_with_the_same_event_id(): void
    {
        $eventId = $this->append('order-1');

        // relay-a gets as far as the broker confirm, then the process is killed.
        foreach ($this->store->claim('relay-a', 10, self::lease()) as $message) {
            $this->broker->publish($message);
        }

        $this->wait(31);
        $this->relay('relay-b')->runOnce();

        $this->assertSame([$eventId, $eventId], $this->broker->publishedEventIds());
        $this->assertSame('published', $this->statusOf($eventId));
    }

    #[Test]
    public function a_verdict_that_arrives_after_the_lease_is_rejected_and_counted(): void
    {
        $eventId = $this->append('order-1');
        [$slow] = $this->store->claim('relay-a', 10, self::lease());

        // relay-a is still waiting on the broker when its lease runs out; relay-b takes over.
        $this->wait(31);
        $this->relay('relay-b')->runOnce();

        $this->assertFalse($this->store->markPublished($slow, 'relay-a'));
        $this->assertSame('published', $this->statusOf($eventId));

        $health = $this->health()->snapshot();
        $this->assertSame(1, $health->total(Counter::Published));
        $this->assertSame(1, $health->total(Counter::LeaseLost));
    }

    #[Test]
    public function a_late_failure_cannot_release_the_lease_another_relay_now_holds(): void
    {
        $eventId = $this->append('order-1');
        [$late] = $this->store->claim('relay-a', 10, self::lease());

        // relay-a's lease runs out mid-publish and relay-b claims the row and is publishing it.
        // Then relay-a's broker call finally times out and relay-a reports a failure.
        $this->wait(31);
        $this->store->claim('relay-b', 10, self::lease());

        $this->assertFalse($this->store->scheduleRetry($late, 'relay-a', Duration::fromSeconds(0), 'timeout', false));

        // Had it succeeded, the row would be unlocked and due, and a third relay could publish it
        // while relay-b still is.
        $this->assertSame('relay-b', $this->row($eventId)['locked_by']);
        $this->assertSame([], $this->store->claim('relay-c', 10, self::lease()));
    }

    #[Test]
    public function the_lease_covers_one_publish_not_the_whole_batch(): void
    {
        $first = $this->append('order-1');
        $second = $this->append('order-2');
        $this->append('order-3');
        $takenOver = null;

        // Each publish takes 20 s against a 30 s lease, so the batch takes 60. Halfway through the
        // second publish, a second relay tries to take the third row.
        $slow = fn (): null => $this->wait(20);
        $this->broker->script($first, $slow);
        $this->broker->script($second, function () use (&$takenOver): void {
            $this->wait(20);
            $takenOver = $this->store->claim('relay-b', 10, self::lease());
        });

        $report = $this->relay()->runOnce();

        $this->assertSame([], $takenOver);
        $this->assertSame(3, $report->count(Counter::Published));
        $this->assertSame(0, $this->health()->snapshot()->total(Counter::LeaseLost));
        // The renewal beats too: 40 s into the pass, the relay is still alive.
        $this->assertSame(1, $this->health()->snapshot()->relaysAlive);
    }

    #[Test]
    public function a_relay_asked_to_stop_releases_what_it_has_not_started(): void
    {
        $first = $this->append('order-1');
        $second = $this->append('order-2');
        $third = $this->append('order-3');
        $stopping = false;
        $this->broker->script($first, function () use (&$stopping): void {
            $stopping = true;   // SIGTERM arrives while the first publish is in flight
        });

        // A closure by reference: an arrow function would capture false once and never see SIGTERM.
        $report = $this->relay('relay-a')->runOnce(static function () use (&$stopping): bool {
            return $stopping;
        });

        $this->assertSame(1, $report->count(Counter::Published));
        foreach ([$second, $third] as $released) {
            $row = $this->row($released);
            $this->assertNull($row['locked_by']);
            $this->assertSame(0, $row['attempts']);
        }

        // The next relay takes them at once, with nothing recorded against them.
        $this->relay('relay-b')->runOnce();
        $this->assertSame(0, $this->row($second)['unexplained_failures']);
        $this->assertSame([$first, $second, $third], $this->broker->publishedEventIds());
    }

    #[Test]
    public function a_rolling_restart_does_not_dead_letter_the_oldest_events(): void
    {
        $oldest = $this->append('order-1');
        $this->append('order-2');

        // Three replicas restart one after another, each asked to stop before its first publish.
        // Abandoned instead of released, the same oldest rows would be first in every claim and
        // would be dead after the third restart.
        foreach (['relay-a', 'relay-b', 'relay-c'] as $restarting) {
            $this->relay($restarting)->runOnce(static fn (): bool => true);
            $this->wait(31);
        }

        $this->relay('relay-d')->runOnce();

        $this->assertSame('published', $this->statusOf($oldest));
        $this->assertSame(0, $this->health()->snapshot()->dead);
    }

    #[Test]
    public function a_transient_failure_is_scheduled_for_later_and_keeps_its_error(): void
    {
        $eventId = $this->append('order-1');
        $this->broker->script($eventId, new TransientPublishFailure('connection refused'));

        $report = $this->relay()->runOnce();

        $row = $this->row($eventId);
        $this->assertSame(1, $report->count(Counter::Retried));
        $this->assertSame('pending', $row['status']);
        $this->assertNull($row['locked_by']);
        $this->assertStringContainsString('connection refused', (string) $row['last_error']);
        $this->assertSame(0, $row['unexplained_failures']);
    }

    #[Test]
    public function a_permanent_failure_is_dead_on_the_first_attempt(): void
    {
        $eventId = $this->append('order-1');
        $this->broker->script($eventId, new PermanentPublishFailure('message too large'));

        $this->relay()->runOnce();

        $row = $this->row($eventId);
        $this->assertSame('dead', $row['status']);
        $this->assertSame(1, $row['attempts']);
        $this->assertSame([], $this->broker->published);
    }

    #[Test]
    public function an_unclassified_exception_is_retried_a_few_times_then_dead(): void
    {
        $eventId = $this->append('order-1');
        $this->broker->script($eventId, new RuntimeException('?'), new RuntimeException('?'), new RuntimeException('?'));

        $relay = $this->relay();
        $relay->runOnce();
        $this->wait(400);
        $relay->runOnce();
        $this->assertSame('pending', $this->statusOf($eventId));

        $this->wait(400);
        $relay->runOnce();

        $row = $this->row($eventId);
        $this->assertSame('dead', $row['status']);
        $this->assertSame(3, $row['attempts']);
        $this->assertStringContainsString('3 unexplained failures', (string) $row['last_error']);
    }

    #[Test]
    public function an_event_that_keeps_killing_the_relay_is_dead_lettered_before_the_broker_sees_it_again(): void
    {
        $eventId = $this->append('order-1');

        // Three relays in a row claim it and never come back (an OOM kill on a huge payload, say).
        foreach (['relay-a', 'relay-b', 'relay-c'] as $doomed) {
            $this->store->claim($doomed, 10, self::lease());
            $this->wait(31);
        }

        $this->relay('relay-d')->runOnce();

        $row = $this->row($eventId);
        $this->assertSame('dead', $row['status']);
        $this->assertSame(4, $row['attempts']);
        $this->assertSame(3, $row['unexplained_failures']);
        $this->assertSame([], $this->broker->published);
    }

    #[Test]
    public function a_transient_failure_past_the_retry_window_is_dead(): void
    {
        $eventId = $this->append('order-1');
        $this->broker->script(
            $eventId,
            new TransientPublishFailure('broker unreachable'),
            new TransientPublishFailure('still unreachable'),
            new TransientPublishFailure('still unreachable'),
        );
        $relay = $this->relay(policy: $this->policy(retryWindow: Duration::fromHours(1)));

        // Three failures half an hour apart: the window runs from the first, so the third is out.
        $relay->runOnce();
        $this->wait(1_800);
        $relay->runOnce();
        $this->assertSame('pending', $this->statusOf($eventId));
        $this->wait(1_800);
        $relay->runOnce();

        $this->assertSame('dead', $this->statusOf($eventId));
        $this->assertStringContainsString('retry window of 3600s exhausted', (string) $this->row($eventId)['last_error']);
    }

    #[Test]
    public function the_retry_window_starts_at_the_first_failure_not_at_the_insert(): void
    {
        // The relay was down for a day; the event waited, it did not fail.
        $eventId = $this->append('order-1');
        $this->wait(90_000);
        $this->broker->script($eventId, new TransientPublishFailure('one-second blip'));

        $this->relay()->runOnce();

        $this->assertSame('pending', $this->statusOf($eventId));
        $this->assertSame(1, $this->health()->snapshot()->retrying);
    }

    #[Test]
    public function a_requeued_event_gets_a_fresh_retry_window(): void
    {
        $eventId = $this->append('order-1');
        $this->broker->script(
            $eventId,
            new TransientPublishFailure('down'),
            new TransientPublishFailure('still down'),
            new TransientPublishFailure('down again'),
        );
        $relay = $this->relay(policy: $this->policy(retryWindow: Duration::fromHours(1)));
        $relay->runOnce();
        $this->wait(3_600);
        $relay->runOnce();
        $this->assertSame('dead', $this->statusOf($eventId));
        $this->assertTrue($this->store->requeue($eventId));
        $this->assertSame(0, $this->health()->snapshot()->retrying);

        $relay->runOnce();

        // Without the reset, retry_since would still be an hour old and this would be dead again.
        $this->assertSame('pending', $this->statusOf($eventId));
        $relay->runOnce();
        $this->wait(1);
        $relay->runOnce();
        $this->assertSame('published', $this->statusOf($eventId));
    }

    #[Test]
    public function a_fifteen_minute_outage_dead_letters_nothing(): void
    {
        $eventId = $this->append('order-1');
        $relay = $this->relay();
        $this->broker->goDown(new TransientPublishFailure('broker unreachable'));
        $elapsed = 0;
        $attemptedAt = [];

        // Stops early if the event dies, so a policy that kills it fails the test instead of
        // looping on a row that will never be due again.
        while ($elapsed < 900 && 'pending' === $this->statusOf($eventId)) {
            $relay->runOnce();
            $attemptedAt[] = $elapsed;
            $wait = $this->secondsUntilDue($eventId);
            $this->wait($wait);
            $elapsed += $wait;
        }

        $this->broker->restore();
        $relay->runOnce();

        $row = $this->row($eventId);
        $this->assertSame('published', $row['status']);
        $this->assertSame(14, $row['attempts']);
        // Seconds into the outage of each failed attempt; the seeded jitter makes them exact.
        // A count limit of 5 dead-letters this healthy event 11 seconds in; a limit of 10, at 259.
        $this->assertSame([0, 1, 3, 6, 11, 22, 46, 95, 152, 259, 374, 611, 839], $attemptedAt);
    }

    private function secondsUntilDue(string $eventId): int
    {
        $statement = $this->pdo->prepare(
            'SELECT ceil(greatest(extract(epoch FROM next_attempt_at - now()), 0))::int FROM outbox WHERE event_id = :event_id',
        );
        $statement->execute(['event_id' => $eventId]);

        return (int) $statement->fetchColumn();
    }
}
