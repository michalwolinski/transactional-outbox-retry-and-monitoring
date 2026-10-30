<?php

declare(strict_types=1);

namespace Outbox\Retry;

use Time\Duration;

/**
 * What happens to a message after a failed attempt: back into the queue after a delay, or out of
 * it for a human to look at.
 */
final readonly class Decision
{
    private function __construct(
        public Outcome $outcome,
        public Duration $delay,
        public string $reason,
        public bool $unexplained,
    ) {
    }

    public static function retry(Duration $delay, string $reason, bool $unexplained = false): self
    {
        return new self(Outcome::Retry, $delay, $reason, $unexplained);
    }

    public static function deadLetter(string $reason): self
    {
        return new self(Outcome::DeadLetter, Duration::fromSeconds(0), $reason, false);
    }
}
