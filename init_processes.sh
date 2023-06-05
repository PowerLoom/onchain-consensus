#!/bin/bash

echo 'starting processes...';
pm2 start pm2.config.js

echo 'processes started';
pm2 logs --lines 1000
