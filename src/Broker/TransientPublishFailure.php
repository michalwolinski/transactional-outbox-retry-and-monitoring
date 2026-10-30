<?php

declare(strict_types=1);

namespace Outbox\Broker;

use RuntimeException;

/**
 * The same message may succeed later. Retried with backoff for as long as the retry window lasts.
 */
final class TransientPublishFailure extends RuntimeException
{
}
