<?php

declare(strict_types=1);

namespace Outbox\Broker;

use RuntimeException;

/**
 * The same message will fail the same way every time. Retrying it only delays the dead letter.
 */
final class PermanentPublishFailure extends RuntimeException
{
}
