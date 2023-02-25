#!/bin/bash

if [ ! -f "settings/settings.json" ]; then
    echo "Settings is not populated, exiting!";
    exit 1;
fi

echo 'starting processes...';
pm2 start pm2.config.js

pm2 logs --lines 1000
