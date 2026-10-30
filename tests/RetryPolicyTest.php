<?php

declare(strict_types=1);

namespace Outbox\Tests;

use Outbox\Broker\PermanentPublishFailure;
use Outbox\Broker\TransientPublishFailure;
use Outbox\OutboxMessage;
use Outbox\Retry\Outcome;
use Outbox\Retry\RetryPolicy;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;
use RuntimeException;
use Time\Duration;

final class RetryPolicyTest extends TestCase
{
    #[Test]
    public function the_ceiling_doubles_from_the_base_and_stops_at_the_cap(): void
    {
        $policy = new RetryPolicy();

        $ceilings = array_map(static fn (int $attempt): int => $policy->ceiling($attempt)->seconds, range(1, 12));

        $this->assertSame([1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300], $ceilings);
    }

    #[Test]
    public function every_delay_lies_between_zero_and_the_ceiling(): void
    {
        $policy = $this->seeded();

        foreach (range(1, 12) as $attempt) {
            $delays = array_map(static fn (): int => $policy->delay($attempt)->seconds, range(1, 500));

            $this->assertGreaterThanOrEqual(0, min($delays));
            $this->assertLessThanOrEqual($policy->ceiling($attempt)->seconds, max($delays));
        }
    }

    #[Test]
    public function full_jitter_breaks_up_a_herd_that_failed_in_the_same_second(): void
    {
        // The broker comes back after an outage; 1000 rows are on their ninth attempt.
        $policy = $this->seeded();
        $perSecond = array_count_values(array_map(static fn (): int => $policy->delay(9)->seconds, range(1, 1000)));

        // Without jitter all 1000 would retry at second 256. With it they land on 253 of the
        // 257 possible seconds, and the busiest one gets 10.
        $this->assertSame(253, count($perSecond));
        $this->assertSame(10, max($perSecond));
    }

    #[Test]
    public function the_three_kinds_of_failure_get_three_different_decisions(): void
    {
        $policy = $this->seeded();
        $message = $this->message(attempts: 1, unexplained: 0, retryAge: 10);

        $permanent = $policy->afterFailure($message, new PermanentPublishFailure('rejected'));
        $transient = $policy->afterFailure($message, new TransientPublishFailure('timeout'));
        $unknown = $policy->afterFailure($message, new RuntimeException('?'));

        $this->assertSame(Outcome::DeadLetter, $permanent->outcome);
        $this->assertSame(Outcome::Retry, $transient->outcome);
        $this->assertFalse($transient->unexplained);
        $this->assertSame(Outcome::Retry, $unknown->outcome);
        $this->assertTrue($unknown->unexplained);
    }

    #[Test]
    public function transient_failures_are_bounded_by_time_and_not_by_count(): void
    {
        $policy = $this->seeded();

        $manyAttemptsYoung = $policy->afterFailure($this->message(attempts: 40, unexplained: 0, retryAge: 3_600), new TransientPublishFailure('down'));
        $fewAttemptsOld = $policy->afterFailure($this->message(attempts: 2, unexplained: 0, retryAge: 86_400), new TransientPublishFailure('down'));

        $this->assertSame(Outcome::Retry, $manyAttemptsYoung->outcome);
        $this->assertSame(Outcome::DeadLetter, $fewAttemptsOld->outcome);
    }

    #[Test]
    public function a_message_with_too_many_unexplained_failures_is_stopped_before_publish(): void
    {
        $policy = $this->seeded();

        $this->assertNull($policy->beforePublish($this->message(attempts: 3, unexplained: 2, retryAge: 0)));
        $this->assertSame(Outcome::DeadLetter, $policy->beforePublish($this->message(attempts: 4, unexplained: 3, retryAge: 0))?->outcome);
    }

    private function seeded(): RetryPolicy
    {
        return new RetryPolicy(new Randomizer(new Xoshiro256StarStar(2026)));
    }

    private function message(int $attempts, int $unexplained, int $retryAge): OutboxMessage
    {
        return new OutboxMessage(1, '0199c0de-0000-7000-8000-000000000001', 'order-1', 'order.placed', '{}', $attempts, $unexplained, Duration::fromSeconds($retryAge));
    }
}
