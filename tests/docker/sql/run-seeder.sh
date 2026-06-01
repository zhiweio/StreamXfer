#!/bin/bash
# StreamXfer test data seeder — runs schema setup then seeds ~30M rows
set -e

SQLCMD=/opt/mssql-tools18/bin/sqlcmd
HOST="${SQL_SERVER_HOST:-sqlserver}"
USER="sa"
PASS="${SA_PASSWORD:-StreamXfer@2024!}"
OPTS="-S $HOST -U $USER -P $PASS -No -l 60 -t 0 -b"

echo "============================================"
echo "StreamXfer Test Data Seeder"
echo "Host: $HOST"
echo "Target: ~30M rows across 8 tables"
echo "Est. time: 10-30 min depending on resources"
echo "============================================"

echo "[1/2] Running schema setup..."
$SQLCMD $OPTS -i /sql/00_setup.sql
echo "Schema setup complete."

echo "[2/2] Running data seeder..."
$SQLCMD $OPTS -d streamxfer_test -i /sql/01_seed.sql
echo "============================================"
echo "Seeding complete!"
