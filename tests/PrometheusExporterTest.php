<?php

declare(strict_types=1);

namespace Outbox\Tests;

use Outbox\Monitoring\HealthSnapshot;
use Outbox\Monitoring\PrometheusExporter;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class PrometheusExporterTest extends TestCase
{
    #[Test]
    public function it_renders_every_gauge_and_counter_once_without_labels(): void
    {
        $snapshot = new HealthSnapshot(
            pending: 12,
            retrying: 3,
            dead: 1,
            held: 2,
            parkedAggregates: 1,
            oldestPendingAgeSeconds: 42.5,
            relaysAlive: 2,
            totals: ['published' => 1200, 'retried' => 40, 'dead_lettered' => 1, 'lease_lost' => 0],
        );

        $text = new PrometheusExporter()->render($snapshot);

        $samples = array_values(array_filter(explode("\n", $text), static fn (string $line): bool => '' !== $line && false === str_starts_with($line, '#')));
        $this->assertSame([
            'outbox_oldest_pending_age_seconds 42.5',
            'outbox_pending_rows 12',
            'outbox_retrying_rows 3',
            'outbox_dead_rows 1',
            'outbox_held_rows 2',
            'outbox_parked_aggregates 1',
            'outbox_relays_alive 2',
            'outbox_published_total 1200',
            'outbox_retried_total 40',
            'outbox_dead_lettered_total 1',
            'outbox_lease_lost_total 0',
        ], $samples);
        $this->assertSame(11, substr_count($text, '# TYPE '));
        $this->assertStringEndsWith("\n", $text);
    }
}
