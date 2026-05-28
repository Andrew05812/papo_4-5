FROM postgres:16

# Установка модуля wal2json — логический декодер PostgreSQL,
# который формирует JSON-представление изменений в WAL-логе.
# Требуется по заданию практики №4.
RUN apt-get update && \
    apt-get install -y postgresql-16-wal2json && \
    rm -rf /var/lib/apt/lists/*

# Копируем init-скрипт (создание таблиц + PUBLICATION)
COPY init.sql /docker-entrypoint-initdb.d/init.sql

# Копируем конфигурацию PostgreSQL (wal_level=logical и др.)
# Файл подключается через командную строку: -c config_file=...
COPY postgresql.conf /etc/postgresql/postgresql.conf

# Запуск PostgreSQL с конфигурацией для CDC
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
