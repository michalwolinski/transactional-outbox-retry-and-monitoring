"""
Removes one mechanism at a time from a copy of the repository and runs the suite against it.
A mutant the suite still passes is a mechanism the tests do not actually prove.

    docker compose up --detach --wait db && docker compose build cli
    python3 mutations/run.py

Expected: every mutant is killed except "mark ignores lease owner", which is equivalent: a late
success marked by the old owner or by the new one leaves the same duplicate and the same counts
(see the README).
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NETWORK = os.path.basename(ROOT) + "_default"
IMAGE = "transactional-outbox:php-8.6.0RC4"
DSN = "pgsql:host=db;port=5432;dbname=outbox;user=outbox;password=outbox"
EQUIVALENT = {"mark ignores lease owner"}

# (name, file, original text, mutated text). The original must occur exactly once.
MUTANTS = [
    (
        'ordering clause removed',
        'src/OutboxStore.php',
        "AND earlier.status <> 'published'",
        'AND false',
    ),
    (
        'crash not counted as unexplained',
        'src/OutboxStore.php',
        '+ CASE WHEN o.locked_by IS NULL THEN 0 ELSE 1 END',
        '+ 0',
    ),
    (
        'attempt not counted at claim',
        'src/OutboxStore.php',
        'SET attempts             = o.attempts + 1,',
        'SET attempts             = o.attempts,',
    ),
    (
        'failure verdict ignores lease owner',
        'src/OutboxStore.php',
        "                locked_by = NULL, locked_until = NULL\n            WHERE id = :id AND locked_by = :relay_id AND status = 'pending'\n            SQL, ['delay'",
        "                locked_by = NULL, locked_until = NULL\n            WHERE id = :id AND CAST(:relay_id AS text) IS NOT NULL AND status = 'pending'\n            SQL, ['delay'",
    ),
    (
        'mark ignores lease owner',
        'src/OutboxStore.php',
        "SET status = 'published', published_at = now(), last_error = NULL,\n                locked_by = NULL, locked_until = NULL\n            WHERE id = :id AND locked_by = :relay_id",
        "SET status = 'published', published_at = now(), last_error = NULL,\n                locked_by = NULL, locked_until = NULL\n            WHERE id = :id AND CAST(:relay_id AS text) IS NOT NULL",
    ),
    (
        'lease ignored by claim',
        'src/OutboxStore.php',
        'AND (o.locked_until IS NULL OR o.locked_until < now())',
        '',
    ),
    (
        'health reads published history',
        'src/Monitoring/OutboxHealth.php',
        "WHERE o.status <> 'published'",
        'WHERE true',
    ),
    (
        'no jitter',
        'src/Retry/RetryPolicy.php',
        'return Duration::fromSeconds($this->random->getInt(0, $this->ceiling($attempt)->seconds));',
        'return $this->ceiling($attempt);',
    ),
    (
        'no cap',
        'src/Retry/RetryPolicy.php',
        'return 0 < Duration::compare($uncapped, $this->maxDelay) ? $this->maxDelay : $uncapped;',
        'return $uncapped;',
    ),
    (
        'no pre-publish suspect check',
        'src/Retry/RetryPolicy.php',
        'if ($message->unexplainedFailures >= $this->maxUnexplainedFailures) {',
        'if (false) {',
    ),
    (
        'no retry window',
        'src/Retry/RetryPolicy.php',
        'if (0 <= Duration::compare($message->retryAge, $this->retryWindow)) {',
        'if (false) {',
    ),
    (
        'transient counted as unexplained',
        'src/Retry/RetryPolicy.php',
        'return Decision::retry($this->delay($message->attempts), $error);',
        'return Decision::retry($this->delay($message->attempts), $error, unexplained: true);',
    ),
    (
        'unknown treated as permanent',
        'src/Retry/RetryPolicy.php',
        'return Decision::retry($this->delay($message->attempts), $error, unexplained: true);',
        'return Decision::deadLetter($error);',
    ),
    (
        'count-based limit of 10',
        'src/Retry/RetryPolicy.php',
        'if (0 <= Duration::compare($message->retryAge, $this->retryWindow)) {',
        'if ($message->attempts >= 10) {',
    ),
    (
        'held rows not counted',
        'src/Monitoring/OutboxHealth.php',
        "AND d.id < o.id AND d.status = 'dead'",
        "AND d.id < o.id AND d.status = 'nope'",
    ),
    (
        'lag from newest instead of oldest',
        'src/Monitoring/OutboxHealth.php',
        'min(o.created_at)',
        'max(o.created_at)',
    ),
    (
        'writer allows no transaction',
        'src/OutboxWriter.php',
        'if (false === $this->pdo->inTransaction()) {',
        'if (false) {',
    ),
    (
        'no lease renewal before each publish',
        'src/Relay.php',
        '$held = $this->store->renew($this->relayId, $unsettled, $this->lease);',
        '$held = $unsettled;',
    ),
    (
        'no per-aggregate writer lock',
        'src/OutboxWriter.php',
        '"SELECT pg_advisory_xact_lock(hashtext(\'outbox\'), hashtext(:aggregate_id))"',
        "'SELECT CAST(:aggregate_id AS text)'",
    ),
    (
        'window restarts at every failure',
        'src/OutboxStore.php',
        'retry_since = coalesce(retry_since, now()),',
        'retry_since = now(),',
    ),
    (
        'window starts at insert',
        'database/schema.sql',
        '    retry_since          TIMESTAMPTZ,',
        '    retry_since          TIMESTAMPTZ DEFAULT now(),',
    ),
    (
        'counter is a plain update',
        'src/OutboxStore.php',
        'INSERT INTO outbox_relay (relay_id, {$column}) VALUES (:relay_id, 1)\n                ON CONFLICT (relay_id) DO UPDATE SET {$column} = outbox_relay.{$column} + 1',
        'UPDATE outbox_relay SET {$column} = {$column} + 1 WHERE relay_id = :relay_id',
    ),
    (
        'retrying counts any old error',
        'src/Monitoring/OutboxHealth.php',
        'o.retry_since IS NOT NULL) AS retrying',
        'o.last_error IS NOT NULL) AS retrying',
    ),
    (
        'requeue keeps old window',
        'src/OutboxStore.php',
        'retry_since = NULL, next_attempt_at = now()',
        'next_attempt_at = now()',
    ),
    (
        'renewal does not beat',
        'src/OutboxStore.php',
        '    public function renew(string $relayId, array $ids, Duration $lease): array\n    {\n        $this->beat($relayId);\n',
        '    public function renew(string $relayId, array $ids, Duration $lease): array\n    {\n',
    ),
    (
        'writer lock without a namespace',
        'src/OutboxWriter.php',
        "pg_advisory_xact_lock(hashtext('outbox'), hashtext(:aggregate_id))",
        'pg_advisory_xact_lock(hashtext(:aggregate_id))',
    ),
    (
        'lease lost not counted',
        'src/OutboxStore.php',
        '$column = $moved ? $counter->value : Counter::LeaseLost->value;',
        '$column = $counter->value;',
    ),
    (
        'stop abandons instead of releasing',
        'src/Relay.php',
        '                $this->store->release($this->relayId, $unsettled);\n',
        '',
    ),
    (
        'release keeps the claimed attempt',
        'src/OutboxStore.php',
        'SET locked_by = NULL, locked_until = NULL, attempts = attempts - 1',
        'SET locked_by = NULL, locked_until = NULL',
    ),
]


def run(name, path, original, mutated, workdir):
    copy = os.path.join(workdir, "repo")
    shutil.rmtree(copy, ignore_errors=True)
    shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns(".git", "vendor", ".phpunit.cache"))
    target = os.path.join(copy, path)
    with open(target) as handle:
        source = handle.read()
    if 1 != source.count(original):
        return f"BROKEN   {name}: the original text occurs {source.count(original)} times in {path}"
    with open(target, "w") as handle:
        handle.write(source.replace(original, mutated))

    command = [
        "docker", "run", "--rm", "--network", NETWORK, "-e", f"OUTBOX_DSN={DSN}",
        "-v", f"{copy}/src:/app/src", "-v", f"{copy}/tests:/app/tests", "-v", f"{copy}/database:/app/database",
        IMAGE, "vendor/bin/phpunit", "--no-progress",
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        return f"TIMEOUT  {name}"

    if 0 == result.returncode:
        return f"SURVIVED {name}"
    killers = re.findall(r"^\d+\) Outbox\\Tests\\(\w+::\w+)", result.stdout, re.MULTILINE)
    return f"KILLED   {name}  <- {', '.join(killers) or '(no failure lines; see phpunit output)'}"


def main():
    workdir = tempfile.mkdtemp(prefix="outbox-mutants-")
    unexpected = []
    try:
        for name, path, original, mutated in MUTANTS:
            line = run(name, path, original, mutated, workdir)
            print(line, flush=True)
            killed = line.startswith("KILLED")
            if killed == (name in EQUIVALENT):
                unexpected.append(name)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    print(f"\n{len(MUTANTS)} mutants, {len(MUTANTS) - len(EQUIVALENT)} expected to be killed")
    if unexpected:
        print("UNEXPECTED: " + ", ".join(unexpected))
        return 1
    print("every mutant behaved as expected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
