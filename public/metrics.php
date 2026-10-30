<?php

declare(strict_types=1);

use Outbox\Database;
use Outbox\Monitoring\OutboxHealth;
use Outbox\Monitoring\PrometheusExporter;

require __DIR__.'/../vendor/autoload.php';

header('Content-Type: text/plain; version=0.0.4; charset=utf-8');

echo new PrometheusExporter()->render(new OutboxHealth(Database::connect())->snapshot());
