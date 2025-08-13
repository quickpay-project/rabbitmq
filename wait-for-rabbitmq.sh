#!/bin/sh
# รอ RabbitMQ ให้พร้อมก่อนรันแอป

host="$1"
shift
cmd="$@"

echo "Waiting for $host..."

until nc -z -v -w30 $host 5672
do
  echo "Waiting for RabbitMQ..."
  sleep 2
done

echo "$host is up, executing command: $cmd"
exec $cmd
