# Kafka Connect — бинарные файлы коннекторов

Директория `kafka-connect/` содержит Dockerfile для сборки образа Kafka Connect.
JAR/zip/tar.gz файлы исключены из Git (см. .gitignore) из-за их размера.

## Как получить бинарные файлы

### Вариант 1: Из работающего контейнера (если пайплайн уже запущен)

```powershell
# Debezium PostgreSQL Connector
docker cp kafka-connect:/usr/share/confluent-hub-components/debezium-debezium-connector-postgresql/lib/. kafka-connect/debezium-libs/
cd kafka-connect/debezium-libs
tar czf ../debezium-plugin.tar.gz *
cd ..
Remove-Item -Recurse debezium-libs

# Redis Connector (stream-reactor)
docker cp kafka-connect:/usr/share/confluent-hub-components/lensesio-kafka-connect-redis/lib/kafka-connect-redis-assembly-6.0.2.jar kafka-connect/redis-connector.jar
# Упаковать в zip для Dockerfile:
Compress-Archive -Path kafka-connect/redis-connector.jar -DestinationPath kafka-connect/redis-connector.zip

# MongoDB Connector (со встроенным CdcHandler)
docker cp kafka-connect:/usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib/mongodb-connector.jar kafka-connect/mongodb-connector.jar

# ElasticSearch Connector
docker cp kafka-connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-elasticsearch/lib/es-connector.jar kafka-connect/es-connector.jar

# Neo4j Connector
docker exec kafka-connect bash -c "cd /usr/share/confluent-hub-components/neo4j-kafka-connect-neo4j/lib && jar cf /tmp/neo4j-connect.zip *.jar"
docker cp kafka-connect:/tmp/neo4j-connect.zip kafka-connect/neo4j-connect.zip
```

### Вариант 2: Скачать из интернета

1. **Debezium PostgreSQL Connector**: https://debezium.io/releases/
   - Скачать debezium-connector-postgres-2.7.1.Final-plugin.tar.gz
   - Переименовать в `debezium-plugin.tar.gz`

2. **Redis Connector (stream-reactor)**: https://github.com/lensesio/stream-reactor/releases
   - Скачать kafka-connect-redis-6.0.2-assembly.jar
   - Упаковать в zip: создать директорию `kafka-connect-redis-6.0.2/`, положить туда JAR, заархивировать

3. **Neo4j Connector**: https://github.com/neo4j-contrib/neo4j-streams/releases
   - Или через confluent-hub: `confluent-hub install neo4j/kafka-connect-neo4j:2.0.0`

4. **ElasticSearch Connector**: https://www.confluent.io/hub/confluentinc/kafka-connect-elasticsearch
   - Или через confluent-hub: `confluent-hub install confluentinc/kafka-connect-elasticsearch:11.1.3`

5. **MongoDB Connector**: https://www.confluent.io/hub/mongodb/kafka-connect-mongodb
   - Или через confluent-hub: `confluent-hub install mongodb/kafka-connect-mongodb:1.12.0`
   - **Важно**: после установки нужно встроить CdcHandler в JAR (см. ниже)

## Встраивание CdcHandler в MongoDB Connector JAR

Исходный код: `lab-4-5/mongo-cdc-handler/src/main/java/com/example/UniversityHierarchyCdcHandler.java`

Отдельный JAR-файл с CdcHandler MongoDB connector НЕ видит (проверено).
Поэтому класс нужно встроить прямо в mongodb-connector.jar:

```powershell
# 1. Скомпилировать CdcHandler (в Docker контейнере с JDK 11, т.к. OneDrive-пути ломают jar tool):
docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:11 bash -c "
  cd lab-4-5/mongo-cdc-handler
  mvn clean package -q
"

# 2. Объединить JAR-файлы (в Docker, т.к. OneDrive unicode-пути ломают jar):
docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:11 bash -c "
  mkdir -p /tmp/jar_merge
  cd /tmp/jar_merge
  jar xf /work/kafka-connect/mongodb-connector-original.jar
  jar xf /work/lab-4-5/mongo-cdc-handler/target/mongo-cdc-handler-1.0.0.jar
  jar cf /work/kafka-connect/mongodb-connector.jar .
"

# 3. Проверить что класс встроен:
jar tf kafka-connect/mongodb-connector.jar | grep UniversityHierarchyCdcHandler
# Должно быть: com/example/UniversityHierarchyCdcHandler.class
```
