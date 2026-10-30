<?php

declare(strict_types=1);

namespace Outbox;

use PDO;

final class Database
{
    public const string DEFAULT_DSN = 'pgsql:host=127.0.0.1;port=55432;dbname=outbox;user=outbox;password=outbox';

    public static function connect(?string $dsn = null): PDO
    {
        $dsn ??= getenv('OUTBOX_DSN') ?: self::DEFAULT_DSN;

        return new PDO($dsn, options: [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]);
    }

    /**
     * Creates the tables if there are none. Never touches a schema that already exists.
     */
    public static function migrate(PDO $pdo): bool
    {
        if (null !== $pdo->query("SELECT to_regclass('outbox')")->fetchColumn()) {
            return false;
        }

        $pdo->exec((string) file_get_contents(__DIR__.'/../database/schema.sql'));

        return true;
    }

    /**
     * Drops and recreates everything. For the tests and the demo, never for a database you keep.
     */
    public static function reset(PDO $pdo): void
    {
        $pdo->exec('DROP TABLE IF EXISTS outbox, outbox_relay');
        $pdo->exec((string) file_get_contents(__DIR__.'/../database/schema.sql'));
    }
}
