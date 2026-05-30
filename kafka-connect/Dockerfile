FROM confluentinc/cp-kafka-connect-base:7.4.0

# ==============================================================================
# Kafka Connect — фреймворк для запуска коннекторов CDC
#
# Установленные коннекторы:
# 1. Debezium PostgreSQL Connector 2.7.1 — source (читает WAL PG → Kafka)
# 2. Redis Sink Connector (stream-reactor 6.0.2) — sink (Kafka → Redis HSET)
#    + DeletingCdcHandler (кастомный Transform для DEL по tombstone)
# 3. Neo4j Sink Connector 5.3.1 — sink (Kafka → Neo4j, cypher strategy)
# 4. MongoDB Sink Connector — sink (Kafka → MongoDB, со встроенным CdcHandler)
# 5. ElasticSearch Sink Connector (Lenses elastic7 10.0.5) — sink (Kafka → ES)
#    + ElasticsearchCdcHandler (кастомный Transform для HTTP DELETE по tombstone)
# ==============================================================================

USER root

RUN microdnf install -y tar gzip unzip

# --- 1. Debezium PostgreSQL Connector ---
COPY debezium-plugin.tar.gz /tmp/debezium-plugin.tar.gz
RUN mkdir -p /usr/share/confluent-hub-components/debezium-debezium-connector-postgresql/lib && \
    mkdir -p /tmp/debezium-extracted && \
    tar xzf /tmp/debezium-plugin.tar.gz -C /tmp/debezium-extracted && \
    cp /tmp/debezium-extracted/debezium-connector-postgres/*.jar /usr/share/confluent-hub-components/debezium-debezium-connector-postgresql/lib/ && \
    rm -rf /tmp/debezium-extracted /tmp/debezium-plugin.tar.gz

# --- 2. Redis Sink Connector (stream-reactor) + DeletingCdcHandler ---
COPY redis-connector.zip /tmp/redis-connector.zip
RUN mkdir -p /usr/share/confluent-hub-components/lensesio-kafka-connect-redis/lib && \
    mkdir -p /tmp/redis-extracted && \
    cd /tmp/redis-extracted && \
    jar xf /tmp/redis-connector.zip && \
    cp /tmp/redis-extracted/kafka-connect-redis-*/kafka-connect-redis-assembly-*.jar /usr/share/confluent-hub-components/lensesio-kafka-connect-redis/lib/ && \
    rm -rf /tmp/redis-extracted /tmp/redis-connector.zip
COPY redis-cdc-handler.jar /usr/share/confluent-hub-components/lensesio-kafka-connect-redis/lib/

# --- 3. Neo4j Sink Connector 5.0.12 ---
COPY neo4j-connect.zip /tmp/neo4j-connect.zip
RUN mkdir -p /usr/share/confluent-hub-components/neo4j-kafka-connect/lib && \
    cd /tmp && jar xf /tmp/neo4j-connect.zip && \
    cp /tmp/*.jar /usr/share/confluent-hub-components/neo4j-kafka-connect/lib/ && \
    rm -rf /tmp/neo4j-connect.zip /tmp/META-INF

# --- 4. MongoDB Sink Connector (со встроенным CdcHandler) ---
COPY mongodb-connector.jar /tmp/mongodb-connector.jar
RUN mkdir -p /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib && \
    cp /tmp/mongodb-connector.jar /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib/ && \
    rm -f /tmp/mongodb-connector.jar

# --- 5. ElasticSearch Sink Connector (Lenses elastic7 10.0.5) + ElasticsearchCdcHandler ---
COPY es-connector.jar /tmp/es-connector.jar
RUN mkdir -p /usr/share/confluent-hub-components/kafka-connect-elastic7-10.0.5/lib && \
    cp /tmp/es-connector.jar /usr/share/confluent-hub-components/kafka-connect-elastic7-10.0.5/lib/ && \
    rm -f /tmp/es-connector.jar
COPY elasticsearch-cdc-handler.jar /usr/share/confluent-hub-components/kafka-connect-elastic7-10.0.5/

USER appuser
