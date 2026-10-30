<?php

declare(strict_types=1);

namespace Outbox\Tests;

use LogicException;
use Outbox\Database;
use Outbox\OutboxWriter;
use Outbox\Tests\Support\OutboxTestCase;
use PDOException;
use PHPUnit\Framework\Attributes\Test;

final class WriterTest extends OutboxTestCase
{
    #[Test]
    public function an_outbox_write_outside_a_transaction_is_refused(): void
    {
        $this->expectException(LogicException::class);

        new OutboxWriter($this->pdo)->append('order-1', 'order.placed', []);
    }

    #[Test]
    public function writers_of_one_aggregate_queue_until_the_earlier_one_commits(): void
    {
        // A slow writer has appended "placed" and not committed yet.
        $this->pdo->beginTransaction();
        new OutboxWriter($this->pdo)->append('order-1', 'order.placed', []);

        // A fast writer for the same order cannot even get an id until it does. Without the wait,
        // its "paid" could commit first and be published before "placed" is visible.
        $fast = Database::connect();
        $fast->exec("SET lock_timeout = '200ms'");
        $fast->beginTransaction();

        try {
            new OutboxWriter($fast)->append('order-1', 'order.paid', []);
            $this->fail('the second writer did not wait');
        } catch (PDOException $exception) {
            $this->assertSame('55P03', $exception->getCode());   // lock_not_available
        } finally {
            $fast->rollBack();
            $this->pdo->rollBack();
        }
    }

    #[Test]
    public function writers_of_different_aggregates_do_not_wait_for_each_other(): void
    {
        $this->pdo->beginTransaction();
        new OutboxWriter($this->pdo)->append('order-1', 'order.placed', []);

        $other = Database::connect();
        $other->exec("SET lock_timeout = '200ms'");
        $other->beginTransaction();
        new OutboxWriter($other)->append('order-2', 'order.placed', []);
        $other->commit();
        $this->pdo->commit();

        $this->assertSame(2, (int) $this->pdo->query('SELECT count(*) FROM outbox')->fetchColumn());
    }

    #[Test]
    public function the_writer_lock_does_not_collide_with_the_applications_own_advisory_locks(): void
    {
        // Some other part of the application holds a one-key advisory lock on the same number.
        $other = Database::connect();
        $other->beginTransaction();
        $other->prepare('SELECT pg_advisory_xact_lock(hashtext(:key))')->execute(['key' => 'order-1']);

        $this->pdo->exec("SET lock_timeout = '200ms'");
        $this->pdo->beginTransaction();
        new OutboxWriter($this->pdo)->append('order-1', 'order.placed', []);
        $this->pdo->commit();
        $other->rollBack();

        $this->assertSame(1, (int) $this->pdo->query('SELECT count(*) FROM outbox')->fetchColumn());
    }

    #[Test]
    public function a_rolled_back_business_transaction_takes_its_event_with_it(): void
    {
        $this->pdo->beginTransaction();
        new OutboxWriter($this->pdo)->append('order-1', 'order.placed', ['total' => 100]);
        $this->pdo->rollBack();

        $this->assertSame(0, (int) $this->pdo->query('SELECT count(*) FROM outbox')->fetchColumn());
    }
}
