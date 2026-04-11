#!/bin/bash
set -e
echo "=== ENTRYPOINT SCRIPT STARTED ==="
# Функция для проверки готовности PostgreSQL
wait_for_postgres() {
    echo "Waiting for PostgreSQL..."
    while ! pg_isready -h postgres -p 5432 -U postgres; do
        sleep 2
    done
    echo "PostgreSQL is ready!"
}

# Функция для проверки готовности ClickHouse через HTTP
wait_for_clickhouse() {
    echo "Waiting for ClickHouse..."
    while ! curl -s -f "http://clickhouse:8123?query=SELECT%201" > /dev/null; do
        sleep 2
    done
    echo "ClickHouse is ready!"
}

wait_for_postgres
wait_for_clickhouse

# Запускаем ваш основной ETL-скрипт
echo "Starting ETL job..."
spark-submit /app/dml.py