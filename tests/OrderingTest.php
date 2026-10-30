<?php

declare(strict_types=1);

namespace Outbox\Tests;

use Outbox\Broker\PermanentPublishFailure;
use Outbox\Broker\TransientPublishFailure;
use Outbox\Tests\Support\OutboxTestCase;
use PHPUnit\Framework\Attributes\Test;

final class OrderingTest extends OutboxTestCase
{
    #[Test]
    public function a_retrying_event_holds_back_its_own_aggregate_and_nobody_else(): void
    {
        $placed = $this->append('order-A', 'order.placed');
        $paid = $this->append('order-A', 'order.paid');
        $other = $this->append('order-B', 'order.placed');
        $this->broker->script($placed, new TransientPublishFailure('timeout'));

        $report = $this->relay()->runOnce();

        $this->assertSame(2, $report->claimed);
        $this->assertSame([$other], $this->broker->publishedEventIds());
        $this->assertSame(0, $this->row($paid)['attempts']);
    }

    #[Test]
    public function a_second_relay_does_not_overtake_an_event_the_first_one_is_holding(): void
    {
        $placed = $this->append('order-A', 'order.placed');
        $paid = $this->append('order-A', 'order.paid');

        [$held] = $this->store->claim('relay-a', 1, self::lease());
        $this->assertSame($placed, $held->eventId);

        $this->assertSame(0, $this->relay('relay-b')->runOnce()->claimed);

        $this->store->markPublished($held, 'relay-a');
        $this->relay('relay-b')->runOnce();

        $this->assertSame([$paid], $this->broker->publishedEventIds());
    }

    #[Test]
    public function a_dead_event_parks_its_aggregate_until_it_is_requeued(): void
    {
        $placed = $this->append('order-A', 'order.placed');
        $paid = $this->append('order-A', 'order.paid');
        $other = $this->append('order-B', 'order.placed');
        $this->broker->script($placed, new PermanentPublishFailure('schema rejected'));
        $relay = $this->relay();

        $relay->runOnce();
        $relay->runOnce();

        $this->assertSame('dead', $this->statusOf($placed));
        $this->assertSame('pending', $this->statusOf($paid));
        $health = $this->health()->snapshot();
        $this->assertSame(1, $health->dead);
        $this->assertSame(1, $health->held);
        $this->assertSame(1, $health->parkedAggregates);

        // Someone fixes the consumer's schema and puts the event back.
        $this->assertTrue($this->store->requeue($placed));
        $relay->runOnce();
        $relay->runOnce();

        $this->assertSame([$other, $placed, $paid], $this->broker->publishedEventIds());
        $this->assertSame(0, $this->health()->snapshot()->parkedAggregates);
    }
}
