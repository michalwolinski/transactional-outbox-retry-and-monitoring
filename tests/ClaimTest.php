<?php

declare(strict_types=1);

namespace Outbox\Tests;

use Outbox\Database;
use Outbox\OutboxMessage;
use Outbox\Tests\Support\OutboxTestCase;
use PHPUnit\Framework\Attributes\Test;

final class ClaimTest extends OutboxTestCase
{
    #[Test]
    public function a_claimed_row_is_invisible_to_other_relays_until_its_lease_runs_out(): void
    {
        $eventId = $this->append('order-1');

        $this->assertCount(1, $this->store->claim('relay-a', 10, self::lease()));
        $this->assertSame([], $this->store->claim('relay-b', 10, self::lease()));

        $this->wait(31);

        $reclaimed = $this->store->claim('relay-b', 10, self::lease());
        $this->assertCount(1, $reclaimed);
        $this->assertSame($eventId, $reclaimed[0]->eventId);
    }

    #[Test]
    public function no_row_lock_is_held_while_the_broker_is_called(): void
    {
        $eventId = $this->append('order-1');
        $other = Database::connect();
        $lockedFromOutside = null;

        // While "publishing", a second connection asks for the row lock and refuses to wait.
        // If the relay held a transaction open across the call, NOWAIT would fail here.
        $this->broker->script($eventId, function (OutboxMessage $message) use ($other, &$lockedFromOutside): void {
            $other->beginTransaction();
            $statement = $other->prepare('SELECT id FROM outbox WHERE id = :id FOR UPDATE NOWAIT');
            $statement->execute(['id' => $message->id]);
            $lockedFromOutside = $statement->fetchColumn();
            $other->rollBack();
        });

        $this->relay()->runOnce();

        $this->assertSame((int) $this->row($eventId)['id'], $lockedFromOutside);
        $this->assertSame('published', $this->statusOf($eventId));
    }

    #[Test]
    public function the_attempt_counts_at_claim_time_not_at_failure_time(): void
    {
        $eventId = $this->append('order-1');

        $this->store->claim('relay-a', 10, self::lease());

        $this->assertSame(1, $this->row($eventId)['attempts']);
    }

    #[Test]
    public function a_claim_that_never_came_back_counts_as_unexplained(): void
    {
        $eventId = $this->append('order-1');

        $this->store->claim('relay-a', 10, self::lease());   // relay-a dies here: no verdict, no release
        $this->wait(31);
        $this->store->claim('relay-b', 10, self::lease());

        $row = $this->row($eventId);
        $this->assertSame(2, $row['attempts']);
        $this->assertSame(1, $row['unexplained_failures']);
        $this->assertSame('relay-b', $row['locked_by']);
    }
}
