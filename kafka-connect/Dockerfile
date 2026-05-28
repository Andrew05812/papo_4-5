FROM confluentinc/cp-kafka-connect-base:7.4.0

# ==============================================================================
# Kafka Connect — фреймворк для запуска коннекторов CDC
#
# Установленные коннекторы:
# 1. Debezium PostgreSQL Connector 2.7.1 — source (читает WAL PG → Kafka)
# 2. Redis Sink Connector (stream-reactor 6.0.2) — sink (Kafka → Redis)
# 3. Neo4j Sink Connector 5.0.12 — sink (Kafka → Neo4j)
# 4. MongoDB Sink Connector — sink (Kafka → MongoDB, со встроенным CdcHandler)
# 5. ElasticSearch Sink Connector — sink (Kafka → ES)
# ==============================================================================

USER root

RUN microdnf install -y tar gzip unzip

# --- 1. Debezium PostgreSQL Connector ---
# tar.gz с JAR-файлами Debezium (connector + dependencies)
COPY debezium-plugin.tar.gz /tmp/debezium-plugin.tar.gz
RUN mkdir -p /usr/share/confluent-hub-components/debezium-debezium-connector-postgresql/lib && \
    mkdir -p /tmp/debezium-extracted && \
    tar xzf /tmp/debezium-plugin.tar.gz -C /tmp/debezium-extracted && \
    cp /tmp/debezium-extracted/debezium-connector-postgres/*.jar /usr/share/confluent-hub-components/debezium-debezium-connector-postgresql/lib/ && \
    rm -rf /tmp/debezium-extracted /tmp/debezium-plugin.tar.gz

# --- 2. Redis Sink Connector (stream-reactor) ---
# zip-архив с assembly JAR (все зависимости в одном файле)
COPY redis-connector.zip /tmp/redis-connector.zip
RUN mkdir -p /usr/share/confluent-hub-components/lensesio-kafka-connect-redis/lib && \
    mkdir -p /tmp/redis-extracted && \
    cd /tmp/redis-extracted && \
    jar xf /tmp/redis-connector.zip && \
    cp /tmp/redis-extracted/kafka-connect-redis-*/kafka-connect-redis-assembly-*.jar /usr/share/confluent-hub-components/lensesio-kafka-connect-redis/lib/ && \
    rm -rf /tmp/redis-extracted /tmp/redis-connector.zip

# --- 3. Neo4j Sink Connector ---
# zip-архив со всеми JAR-зависимостями
COPY neo4j-connect.zip /tmp/neo4j-connect.zip
RUN mkdir -p /usr/share/confluent-hub-components/neo4j-kafka-connect-neo4j/lib && \
    mkdir -p /tmp/neo4j-extracted && \
    cd /tmp/neo4j-extracted && \
    jar xf /tmp/neo4j-connect.zip && \
    cp /tmp/neo4j-extracted/*.jar /usr/share/confluent-hub-components/neo4j-kafka-connect-neo4j/lib/ && \
    rm -rf /tmp/neo4j-extracted /tmp/neo4j-connect.zip

# --- 4. MongoDB Sink Connector (со встроенным CdcHandler) ---
# Кастомный JAR: стандартный MongoDB connector + UniversityHierarchyCdcHandler.class внутри.
# Отдельный JAR-файл с CdcHandler НЕ работает — MongoDB connector его не видит,
# поэтому класс вшит прямо в mongodb-connector.jar (подтверждено однокурсниками).
COPY mongodb-connector.jar /tmp/mongodb-connector.jar
RUN mkdir -p /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib && \
    cp /tmp/mongodb-connector.jar /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib/ && \
    rm -f /tmp/mongodb-connector.jar

# --- 5. ElasticSearch Sink Connector ---
COPY es-connector.jar /tmp/es-connector.jar
RUN mkdir -p /usr/share/confluent-hub-components/confluentinc-kafka-connect-elasticsearch/lib && \
    cp /tmp/es-connector.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-elasticsearch/lib/ && \
    rm -f /tmp/es-connector.jar

USER appuser
