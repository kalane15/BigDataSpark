#!/bin/bash
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

    if ! command -v curl &> /dev/null; then
        echo "ERROR: curl not found. Please install curl in the Dockerfile."
        exit 1
    fi

    while true; do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://clickhouse:8123?query=SELECT%201" \
            --user spark_user:spark_password --connect-timeout 5)
        CURL_EXIT=$?

        echo "Curl exit code: $CURL_EXIT, HTTP status: $HTTP_CODE"

        if [ $CURL_EXIT -eq 0 ] && [ "$HTTP_CODE" = "200" ]; then
            echo "ClickHouse is ready!"
            break
        else
            echo "ClickHouse not ready, retrying in 2 seconds..."
            sleep 2
        fi
    done
}
wait_for_postgres
wait_for_clickhouse

cd /opt/spark/work-dir
# Запускаем ваш основной ETL-скрипт
echo "Starting ETL job..."
/opt/spark/bin/spark-submit dml.py