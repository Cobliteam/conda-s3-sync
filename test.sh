#!/bin/sh

set -e

flake8 conda_s3_sync
coverage erase
coverage run --source conda_s3_sync -m py.test
coverage report --include='conda_s3_sync/**' --omit='conda_s3_sync/test/**'
