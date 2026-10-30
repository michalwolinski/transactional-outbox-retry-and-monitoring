<?php

declare(strict_types=1);

namespace Outbox;

/**
 * The relay's counters. Each case is a column of outbox_relay and a `_total` series on /metrics.
 */
enum Counter: string
{
    case Published = 'published';
    case Retried = 'retried';
    case DeadLettered = 'dead_lettered';
    case LeaseLost = 'lease_lost';
}
