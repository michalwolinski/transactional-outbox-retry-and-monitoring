# PHP 8.6 is still a release candidate. Exact tag and digest: the suite's claims are about one
# interpreter and one database version.
FROM php:8.6.0RC4-cli-alpine@sha256:c9c90988b028655d7601a8d780ab5e068abd1c5f430cc0ee642755b94dd46583

# pdo_pgsql for the application; mbstring only because PHPUnit requires it. mbregex is left out:
# it is deprecated in 8.6 along with its Oniguruma dependency.
RUN apk add --no-cache postgresql-libs unzip \
 && apk add --no-cache --virtual .build-deps postgresql-dev \
 && docker-php-ext-configure mbstring --disable-mbregex \
 && docker-php-ext-install -j"$(nproc)" pdo_pgsql mbstring \
 && apk del .build-deps

COPY --from=composer:2.11.0@sha256:c7f68260503e3c1f0ece10bdbe84519fd98d48ba091f2c02f16ea94a7faae337 /usr/bin/composer /usr/bin/composer

WORKDIR /app

COPY composer.json composer.lock ./
RUN composer install --no-interaction --no-progress --no-scripts

COPY . .

CMD ["php", "bin/outbox", "demo"]
