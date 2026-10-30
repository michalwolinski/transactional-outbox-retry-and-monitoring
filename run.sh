#!/usr/bin/env bash
# Everything the article claims, in one command: the PHP suite against PostgreSQL 18, the style
# check, the exporter's output through promtool, the alert rules through their own unit tests,
# and the demo. Exits non-zero on the first thing that disagrees.
set -euo pipefail
cd "$(dirname "$0")"

PROMETHEUS='prom/prometheus:v3.15.1@sha256:fa3302279710be2f21a8301ec17a8b4f2301fd829bd113839b3b6b02b6f191a2'

docker compose build --quiet cli
docker compose up --detach --wait db

echo '== phpunit'
docker compose run --rm cli vendor/bin/phpunit

echo '== pint'
docker compose run --rm cli vendor/bin/pint --test

echo '== demo'
docker compose run --rm cli php bin/outbox demo

echo '== promtool check metrics'
docker compose run --rm -T cli php bin/outbox metrics \
  | docker run --rm -i --entrypoint promtool "$PROMETHEUS" check metrics

echo '== promtool check rules / test rules'
docker run --rm -v "$PWD/monitoring:/monitoring" -w /monitoring --entrypoint promtool "$PROMETHEUS" check rules alerts.yml
docker run --rm -v "$PWD/monitoring:/monitoring" -w /monitoring --entrypoint promtool "$PROMETHEUS" test rules alerts.test.yml
