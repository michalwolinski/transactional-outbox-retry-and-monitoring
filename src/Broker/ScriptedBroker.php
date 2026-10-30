<?php

declare(strict_types=1);

namespace Outbox\Broker;

use Closure;
use Outbox\OutboxMessage;
use Throwable;

/**
 * An in-memory broker for the tests and the demo. Failures are scripted per event type or per
 * event id; everything that gets through is recorded, duplicates included, in publish order.
 */
final class ScriptedBroker implements Broker
{
    /** @var list<OutboxMessage> */
    public private(set) array $published = [];

    /** @var array<string, list<Throwable|Closure(OutboxMessage): void>> */
    private array $script = [];

    private ?Throwable $outage = null;

    /**
     * The next calls for this key (an event id or an event type) do these things, in order.
     * A Throwable is thrown; a Closure runs and the publish then succeeds.
     */
    public function script(string $key, Throwable|Closure ...$steps): void
    {
        $this->script[$key] = [...$this->script[$key] ?? [], ...array_values($steps)];
    }

    /**
     * Every publish fails with $failure until restore().
     */
    public function goDown(Throwable $failure): void
    {
        $this->outage = $failure;
    }

    public function restore(): void
    {
        $this->outage = null;
    }

    public function publish(OutboxMessage $message): void
    {
        if (null !== $this->outage) {
            throw $this->outage;
        }

        foreach ([$message->eventId, $message->eventType] as $key) {
            if ([] === ($this->script[$key] ?? [])) {
                continue;
            }

            $step = array_shift($this->script[$key]);

            if ($step instanceof Throwable) {
                throw $step;
            }

            $step($message);

            break;
        }

        $this->published[] = $message;
    }

    /**
     * @return list<string>
     */
    public function publishedEventIds(): array
    {
        return array_map(static fn (OutboxMessage $message): string => $message->eventId, $this->published);
    }
}
