<?php

declare(strict_types=1);

namespace Outbox;

use Closure;
use Outbox\Broker\Broker;
use Outbox\Retry\Outcome;
use Outbox\Retry\RetryPolicy;
use Throwable;
use Time\Duration;

/**
 * One pass: beat, claim, publish each claimed row outside any transaction, record the verdict.
 *
 * $relayId should be stable across restarts (a pod name, a slot number): every id gets a row in
 * outbox_relay, and the exporter sums their counters, so a row per process id grows forever.
 */
final readonly class Relay
{
    public function __construct(
        private OutboxStore $store,
        private Broker $broker,
        private RetryPolicy $policy,
        public string $relayId,
        private Duration $lease,
        private int $batchSize = 100,
    ) {
    }

    /**
     * @param  (Closure(): bool)|null  $shouldStop  checked before every publish; wire it to SIGTERM.
     *                                              A relay asked to stop finishes the publish it is
     *                                              in and releases the rest of its batch.
     */
    public function runOnce(?Closure $shouldStop = null): RelayReport
    {
        $this->store->beat($this->relayId);
        $report = new RelayReport();

        $batch = $this->store->claim($this->relayId, $this->batchSize, $this->lease);
        $unsettled = array_map(static fn (OutboxMessage $message): int => $message->id, $batch);

        foreach ($batch as $message) {
            if (null !== $shouldStop && $shouldStop()) {
                $this->store->release($this->relayId, $unsettled);

                break;
            }

            $report->claimed++;
            $held = $this->store->renew($this->relayId, $unsettled, $this->lease);
            $unsettled = array_values(array_diff($unsettled, [$message->id]));

            // Taken over while earlier rows of the batch were being published. Skipping it is the
            // one case where a lapsed lease costs nothing: the new owner publishes it, once.
            if (false === in_array($message->id, $held, true)) {
                continue;
            }

            $suspect = $this->policy->beforePublish($message);

            if (null !== $suspect) {
                $this->settle($report, $this->store->deadLetter($message, $this->relayId, $suspect->reason), Counter::DeadLettered);

                continue;
            }

            try {
                $this->broker->publish($message);
            } catch (Throwable $failure) {
                $decision = $this->policy->afterFailure($message, $failure);

                if (Outcome::DeadLetter === $decision->outcome) {
                    $this->settle($report, $this->store->deadLetter($message, $this->relayId, $decision->reason), Counter::DeadLettered);
                } else {
                    $this->settle($report, $this->store->scheduleRetry($message, $this->relayId, $decision->delay, $decision->reason, $decision->unexplained), Counter::Retried);
                }

                continue;
            }

            // The broker has the message. If this UPDATE never runs, the lease expires and the
            // message goes out again: at-least-once, which is why the event id travels with it.
            $this->settle($report, $this->store->markPublished($message, $this->relayId), Counter::Published);
        }

        return $report;
    }

    private function settle(RelayReport $report, bool $moved, Counter $counter): void
    {
        $report->add($moved ? $counter : Counter::LeaseLost);
    }
}
