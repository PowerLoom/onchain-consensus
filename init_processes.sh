#!/bin/bash

source .env

if [ ! -f "settings/settings.json" ]; then
    echo "Settings is not populated, exiting!";
    exit 1;
fi

if [ -z "$UUID" ]; then
    echo "UUID not found in .env - autopopulating to 'generated-uuid'!";
fi

export uuid="${UUID:-generated-uuid}"
echo "UUID set to $uuid";

echo 'starting processes...';
pm2 start pm2.config.js

poetry run python -m snapshotter_cli add-snapshotter "{\"rate_limit\": \"6000000/day;10000/minute;600/second\", \"uuid\": \"$uuid\", \"active\": \"active\", \"name\": \"generated 1\", \"email\": \"generated1@example.com\", \"alias\": \"generated-1\"}";

pm2 logs --lines 1000
