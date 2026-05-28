@echo off
echo Регистрация коннекторов Kafka Connect...
echo.

set CONNECT_URL=http://localhost:8083
set DIR=%~dp0connectors

echo [1/6] Debezium PostgreSQL Source...
curl -s -X POST %CONNECT_URL%/connectors -H "Content-Type: application/json" -d "{\"name\":\"debezium-postgres-source\",\"config\":$(type \"%DIR%\\debezium-postgres-source.json\")}" | python -c "import sys,json; d=json.load(sys.stdin); print('OK' if 'name' in d else d.get('error',d))"

echo [2/6] ElasticSearch Sink...
curl -s -X POST %CONNECT_URL%/connectors -H "Content-Type: application/json" -d "{\"name\":\"elasticsearch-sink\",\"config\":$(type \"%DIR%\\elasticsearch-sink.json\")}" | python -c "import sys,json; d=json.load(sys.stdin); print('OK' if 'name' in d else d.get('error',d))"

echo [3/6] Redis Sink (stream-reactor)...
curl -s -X POST %CONNECT_URL%/connectors -H "Content-Type: application/json" -d "{\"name\":\"redis-sink\",\"config\":$(type \"%DIR%\\redis-sink.json\")}" | python -c "import sys,json; d=json.load(sys.stdin); print('OK' if 'name' in d else d.get('error',d))"

echo [4/6] Neo4j Sink...
curl -s -X POST %CONNECT_URL%/connectors -H "Content-Type: application/json" -d "{\"name\":\"neo4j-sink\",\"config\":$(type \"%DIR%\\neo4j-sink.json\")}" | python -c "import sys,json; d=json.load(sys.stdin); print('OK' if 'name' in d else d.get('error',d))"

echo [5/6] MongoDB Sink (flat)...
curl -s -X POST %CONNECT_URL%/connectors -H "Content-Type: application/json" -d "{\"name\":\"mongodb-sink-flat\",\"config\":$(type \"%DIR%\\mongodb-sink-flat.json\")}" | python -c "import sys,json; d=json.load(sys.stdin); print('OK' if 'name' in d else d.get('error',d))"

echo [6/6] MongoDB Sink (hierarchy)...
curl -s -X POST %CONNECT_URL%/connectors -H "Content-Type: application/json" -d "{\"name\":\"mongodb-sink-hierarchy\",\"config\":$(type \"%DIR%\\mongodb-sink-hierarchy.json\")}" | python -c "import sys,json; d=json.load(sys.stdin); print('OK' if 'name' in d else d.get('error',d))"

echo.
echo Ожидание запуска коннекторов (30 сек)...
timeout /t 30 /nobreak >nul

echo.
echo Статус коннекторов:
for %%c in (debezium-postgres-source elasticsearch-sink redis-sink neo4j-sink mongodb-sink-flat mongodb-sink-hierarchy) do (
    echo %%c: 
    curl -s %CONNECT_URL%/connectors/%%c/status | python -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])"
)

echo.
echo Готово!
