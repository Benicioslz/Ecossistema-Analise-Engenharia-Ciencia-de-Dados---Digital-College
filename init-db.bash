#!/bin/bash

echo "Aguardando SQL Server..."
sleep 30

/opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "$MSSQL_SA_PASSWORD" \
  -C \
  -i /init.sql

chmod +x init-db.sh