# Transactional outbox: retry and monitoring

> Companion code for [Transactional Outbox: Retry and Monitoring](https://wolinski.com/blog/transactional-outbox-retry-and-monitoring.html)
> by [Michał Woliński](https://wolinski.com).

The [first outbox article](https://wolinski.com/blog/outbox-pattern.html) wrote the event in the
same transaction as the business data and drained the table with a polling relay. This repository
is the part that article gave one sentence to: what the relay does when publishing fails, and how
you know from the outside whether it is working.

Plain PHP and PDO, no framework, a scripted in-memory broker, a real PostgreSQL. Every code block in
the article is a file here.

## Versions

| Component | Version | Why this one |
| --- | --- | --- |
| PHP | **8.6.0RC4** (image pinned by digest) | newest tag that exists; `Time\Duration` is the one 8.6 feature the code uses |
| PostgreSQL | **18.6** (image pinned by digest) | `uuidv7()` is built in from 18; the planner assertions are only as stable as the version they ran on |
| `phpunit/phpunit` | 13.4.5 | the suite fails on deprecations, notices and warnings |
| `laravel/pint` | 1.32.1 | `psr12` plus Yoda conditions and ordered imports |
| `promtool` | 3.15.1 (`prom/prometheus` pinned by digest) | lints `/metrics`, checks and unit-tests the alert rules |
| `ext-mbstring` | image | required by PHPUnit, not by the application; built without mbregex |

## Run everything

```bash
./run.sh                     # phpunit, pint, the demo, promtool check metrics/rules, promtool test rules
python3 mutations/run.py     # 29 mutants against the suite: 28 killed, 1 equivalent (below)
```

Or piece by piece:

```bash
docker compose up --detach --wait db
docker compose build cli
docker compose run --rm cli vendor/bin/phpunit       # 40 tests, 165 assertions
docker compose run --rm cli php bin/outbox demo      # the lifecycle in real time, ~11 s; DROPS the tables first
docker compose up metrics                            # then: curl localhost:9464/metrics
```

`bin/outbox` also has `migrate` (creates the tables only if there are none), `metrics`,
`dead-letters` and `requeue <event-id>`. There is deliberately no `relay` command: the only broker
here is the in-memory one, and a relay loop wired to it would mark real rows as published without
sending them anywhere. A deployment runs `Relay::runOnce($shouldStop)` in a loop with its own `Broker`
adapter, a relay id that is stable across restarts, and `$shouldStop` wired to `SIGTERM`, so a
deploy releases the rows it has not started instead of abandoning them.

### Checking the image runs the locked dependencies

`.dockerignore` keeps the host's `vendor/` out of the build context; without it, `COPY . .` puts it
over the one `composer install` just made, and the container quietly runs whatever the host last
installed. The quick check is the platform requirement Composer baked in:

```bash
docker compose run --rm cli grep -o 'PHP_VERSION_ID >= [0-9]*' vendor/composer/platform_check.php
# PHP_VERSION_ID >= 80600
```

## What is where

| Path | What it does |
| --- | --- |
| `database/schema.sql` | the outbox table with retry and lease columns, two partial indexes, the per-relay heartbeat and counters |
| `src/OutboxWriter.php` | one INSERT, refused outside a transaction, serialised per aggregate by an advisory lock |
| `src/OutboxStore.php` | claim with a lease, renew it, verdicts conditional on still holding it, requeue |
| `src/Relay.php` | one pass: beat, claim, renew before each publish, publish with no transaction open, record the verdict; on stop, release the rest |
| `src/Retry/RetryPolicy.php` | permanent → dead; transient → full-jitter backoff inside a time window; unexplained → small count |
| `src/Broker/` | the adapter contract (two exception types) and the scripted broker |
| `src/Monitoring/` | the health query and the Prometheus text exporter |
| `public/metrics.php` | the scrape target |
| `monitoring/alerts.yml`, `alerts.test.yml` | six alert rules and their promtool unit tests |
| `mutations/run.py` | removes one mechanism at a time and checks the suite notices |

## Claims and the tests that hold them

| Claim in the article | Test |
| --- | --- |
| an outbox write outside a transaction is refused; a rollback takes the event with it | `WriterTest` |
| writers of one aggregate wait for each other's commit; different aggregates do not; the lock is namespaced away from the application's own advisory locks | `WriterTest::writers_of_one_aggregate_…`, `…_different_aggregates_…`, `…_does_not_collide_with_the_applications_own_advisory_locks` |
| the lease covers one publish, not the batch; the renewal keeps the heartbeat alive | `RelayTest::the_lease_covers_one_publish_not_the_whole_batch` |
| no row lock is held while the broker is called | `ClaimTest::no_row_lock_is_held_while_the_broker_is_called` |
| a claimed row is invisible to other relays until its lease runs out | `ClaimTest::a_claimed_row_is_invisible_…` |
| the attempt counts at claim time; an abandoned claim counts as unexplained | `ClaimTest::the_attempt_counts_…`, `…_never_came_back_…` |
| a relay asked to stop releases what it has not started; three rolling restarts dead-letter nothing | `RelayTest::a_relay_asked_to_stop_…`, `…a_rolling_restart_does_not_dead_letter_…` |
| a relay killed between confirm and mark causes a duplicate with the same event id | `RelayTest::a_relay_that_dies_between_publish_and_mark_…` |
| a late verdict is rejected and counted as `lease_lost` | `RelayTest::a_verdict_that_arrives_after_the_lease_…` |
| a late failure cannot release a lease another relay holds | `RelayTest::a_late_failure_cannot_release_…` |
| permanent → dead at once; unclassified → dead after 3; a relay-killing event → dead before publish | `RelayTest` |
| transient retries are bounded by time since the **first failure**, not since the insert; a requeued event gets a fresh window and is not counted as retrying | `RelayTest::the_retry_window_starts_at_the_first_failure_…`, `…past_the_retry_window_is_dead`, `…requeued_event_gets_a_fresh_retry_window`, `RetryPolicyTest` |
| a 15-minute outage dead-letters nothing; attempts at 0, 1, 3, 6, **11**, 22, 46, 95, 152, **259**, … s | `RelayTest::a_fifteen_minute_outage_dead_letters_nothing` |
| full jitter spreads 1000 rows over 253 seconds, at most 10 in one | `RetryPolicyTest::full_jitter_breaks_up_a_herd_…` |
| a retrying or dead event holds back its own aggregate and nobody else's, across relays | `OrderingTest` |
| lag is the age of the oldest pending event; a backlog count cannot see it | `HealthTest::lag_is_the_age_of_the_oldest_event_…` |
| counters live in the database and survive a relay restart | `HealthTest::counters_are_summed_…` |
| claim and health queries never scan the published history (200k rows) | `HealthTest::neither_the_claim_nor_the_health_query_…` |
| the 3 a.m. relay death pages via the heartbeat while lag stays zero | `monitoring/alerts.test.yml` |

## The one surviving mutant

`mutations/run.py` removes the lease-owner check from `markPublished`, and the suite stays green.
That mutant is equivalent: when a slow relay's *success* arrives after another relay re-claimed the
row, either the old owner marks it and the new one's mark is lost, or the other way round. Both
orders publish the event twice, record one `published` and one `lease_lost`, and leave the row
published. The owner check matters for a late *failure*, which would otherwise unlock a row another
relay is still publishing — that mutant is killed by `a_late_failure_cannot_release_…`.

## Author

[Michał Woliński](https://wolinski.com)

## License

MIT — see [LICENSE](LICENSE).
