FROM mongo:7

# Кастомный контейнер MongoDB для CDC-пайплайна
# (по заданию: Redis/ES/Neo4j — заводские, а у MongoDB — свой контейнер)
#
# Этот контейнер нужен потому, что MongoDB Kafka Connector использует
# кастомный CdcHandler (UniversityHierarchyCdcHandler), который вшит
# в JAR-файл коннектора. LABEL ниже помечает контейнер как кастомный.

LABEL maintainer="University CDC Pipeline"
LABEL description="Custom MongoDB container for CDC pipeline with UniversityHierarchyCdcHandler"
