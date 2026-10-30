<?php

declare(strict_types=1);

namespace Outbox\Broker;

use Outbox\OutboxMessage;

/**
 * The adapter's contract is the classification, not just the call: a failure the broker can
 * explain is thrown as one of the two exceptions below. Anything else reaching the relay is
 * treated as unexplained.
 */
interface Broker
{
    /**
     * Returns only once the broker has confirmed the message (publisher confirm, acks=all, a 2xx
     * from the API). Returning earlier turns "published" into "handed to a socket".
     *
     * @throws TransientPublishFailure the broker may take this message later: unreachable,
     *                                 timed out, throttled, failing over
     * @throws PermanentPublishFailure the broker will never take this message: rejected payload,
     *                                 unroutable, too large, not authorised for this topic
     */
    public function publish(OutboxMessage $message): void;
}
