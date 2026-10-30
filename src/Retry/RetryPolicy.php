<?php

declare(strict_types=1);

namespace Outbox\Retry;

use Outbox\Broker\PermanentPublishFailure;
use Outbox\Broker\TransientPublishFailure;
use Outbox\OutboxMessage;
use Random\Randomizer;
use Throwable;
use Time\Duration;

/**
 * Three kinds of failure, three budgets:
 *
 *  - transient: retried with capped exponential backoff and full jitter, for as long as the retry
 *    window lasts — a budget in time, because an outage is measured in time;
 *  - permanent: dead at once, the first failure already says everything;
 *  - unexplained (an unclassified exception, or an attempt that never came back): a small count,
 *    because nobody knows whether waiting helps.
 */
final readonly class RetryPolicy
{
    public Duration $baseDelay;

    public Duration $maxDelay;

    public Duration $retryWindow;

    /**
     * The durations default to 1 s, 5 min and 24 h. A static call is not a constant expression,
     * so they cannot be written as parameter defaults.
     */
    public function __construct(
        private Randomizer $random = new Randomizer(),
        ?Duration $baseDelay = null,
        ?Duration $maxDelay = null,
        ?Duration $retryWindow = null,
        public int $maxUnexplainedFailures = 3,
    ) {
        $this->baseDelay = $baseDelay ?? Duration::fromSeconds(1);
        $this->maxDelay = $maxDelay ?? Duration::fromMinutes(5);
        $this->retryWindow = $retryWindow ?? Duration::fromHours(24);
    }

    /**
     * Checked right after the claim, before the broker is called. A message whose previous
     * attempts never came back is the prime suspect for whatever killed them.
     */
    public function beforePublish(OutboxMessage $message): ?Decision
    {
        if ($message->unexplainedFailures >= $this->maxUnexplainedFailures) {
            return Decision::deadLetter(sprintf(
                'gave up after %d unexplained failures (a relay that never came back counts as one)',
                $message->unexplainedFailures,
            ));
        }

        return null;
    }

    public function afterFailure(OutboxMessage $message, Throwable $failure): Decision
    {
        $error = sprintf('%s: %s', $failure::class, $failure->getMessage());

        if ($failure instanceof PermanentPublishFailure) {
            return Decision::deadLetter($error);
        }

        if ($failure instanceof TransientPublishFailure) {
            if (0 <= Duration::compare($message->retryAge, $this->retryWindow)) {
                return Decision::deadLetter(sprintf('retry window of %ds exhausted; last error %s', $this->retryWindow->seconds, $error));
            }

            return Decision::retry($this->delay($message->attempts), $error);
        }

        if ($message->unexplainedFailures + 1 >= $this->maxUnexplainedFailures) {
            return Decision::deadLetter(sprintf('gave up after %d unexplained failures; last error %s', $message->unexplainedFailures + 1, $error));
        }

        return Decision::retry($this->delay($message->attempts), $error, unexplained: true);
    }

    /**
     * Full jitter: anywhere between zero and the ceiling. Not ceiling ± 10% — after an outage every
     * row failed in the same second, and a narrow jitter keeps them in the same few seconds.
     */
    public function delay(int $attempt): Duration
    {
        return Duration::fromSeconds($this->random->getInt(0, $this->ceiling($attempt)->seconds));
    }

    /**
     * base · 2^(attempt − 1), capped. Attempt 1 waits up to base, attempt 2 up to 2·base, …
     */
    public function ceiling(int $attempt): Duration
    {
        $uncapped = $this->baseDelay->multiplyBy(2 ** min(max($attempt - 1, 0), 30));

        return 0 < Duration::compare($uncapped, $this->maxDelay) ? $this->maxDelay : $uncapped;
    }
}
