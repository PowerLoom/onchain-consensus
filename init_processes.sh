#!/bin/bash

if [ -f ".env" ]; then
    source .env
fi

if [ ! -f "settings/settings.json" ]; then
    echo "Settings is not populated, exiting!";
    exit 1;
fi

if [ -z "$UUID" ]; then
    echo "UUID not found in .env - autopopulating to 'generated-uuid'!";
fi

export uuid="${UUID:-generated-uuid}"
echo "UUID set to $uuid";
export name="${NAME:-generated-name}"
echo "Name set to $name";
export alias="${ALIAS:-generated-alias}"
echo "Alias set to $alias";
export email="${EMAIL:-generated@example.com}"
echo "Email set to $email";
echo "{\"rate_limit\": \"6000000/day;10000/minute;600/second\", \"uuid\": \"$uuid\", \"active\": \"active\", \"name\": \"$name\", \"email\": \"$email\", \"alias\": \"$alias\"}";

echo 'starting processes...';
pm2 start pm2.config.js

poetry run python -m snapshotter_cli add-snapshotter "{\"rate_limit\": \"6000000/day;10000/minute;600/second\", \"uuid\": \"$uuid\", \"active\": \"active\", \"name\": \"$name\", \"email\": \"$email\", \"alias\": \"$alias\"}";

pm2 logs --lines 1000
