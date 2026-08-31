#!/bin/bash
set -e

# Membuat Database Engine (Catalog) PostgreSQL di dalam ClickHouse secara dinamis
# Memanfaatkan Environment Variables agar password tidak perlu di-hardcode
clickhouse-client -q "
CREATE DATABASE IF NOT EXISTS pg_catalog 
ENGINE = PostgreSQL('postgres:5432', '${POSTGRES_DB}', '${POSTGRES_USER}', '${POSTGRES_PASSWORD}', 'public');
"
echo "✅ ClickHouse PostgreSQL Database Engine (pg_catalog) berhasil dibuat!"
