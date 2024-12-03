#!/bin/bash
set -e

# Esperar a que el webserver esté disponible
echo "Esperando a que el Airflow Webserver esté listo..."
until curl -s http://127.0.0.1:8080/health | grep '"status":"healthy"' > /dev/null; do
  sleep 5
done

echo "Creando conexión 'mysql_root' en Airflow..."
airflow connections add 'mysql_root' \
    --conn-type 'mysql' \
    --conn-login "$MYSQL_USER" \
    --conn-password "$MYSQL_PASSWORD" \
    --conn-host '127.0.0.1' \
    --conn-port '3306'

echo "Conexión 'mysql_root' creada exitosamente."
