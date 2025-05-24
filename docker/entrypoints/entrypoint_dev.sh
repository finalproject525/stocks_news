#!/bin/sh

# Ensure /etc/environment exists
touch /etc/environment

# Export selected environment variables to /etc/environment
printenv | grep -E '^(AWS_|POSTGRES_|MINIO_|AIRFLOW_)' >> /etc/environment

# Start SSH daemon in foreground
exec /usr/sbin/sshd -D
