#!/bin/bash
# wait-for-manager.sh

# Allows extra arguments passed to postgres. Was once proposed via PR but never excepted and deleted as staled:
# https://github.com/citusdata/docker/pull/338

set -e

until test -f /healthcheck/manager-ready ; do
  >&2 echo "Manager is not ready - sleeping"
  sleep 1
done

>&2 echo "Manager is up - starting worker"

exec gosu postgres "/usr/local/bin/docker-entrypoint.sh" "postgres" "$@"
