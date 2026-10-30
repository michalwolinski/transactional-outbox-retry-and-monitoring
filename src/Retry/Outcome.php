<?php

declare(strict_types=1);

namespace Outbox\Retry;

enum Outcome
{
    case Retry;
    case DeadLetter;
}
