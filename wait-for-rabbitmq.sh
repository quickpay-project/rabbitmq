#!/bin/sh
# wait-for-rabbitmq.sh

set -e

host="$1"
shift
cmd="$@"

echo "⏳ Waiting for RabbitMQ at $host ..."

until nc -z "$host" 5672; do
  >&2 echo "RabbitMQ is unavailable - sleeping"
  sleep 2
done

>&2 echo "RabbitMQ is up - executing command"
exec $cmd
