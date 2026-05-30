# Инструкция для защиты лабораторных работ 4-5

Все команды выполняются в **PowerShell** из папки `lab/`.
Комментарии после `#` — ожидаемый результат.

---

## ШАГ 1. Пустой PostgreSQL (начальное состояние)

Показываем преподавателю, что PostgreSQL пуст — данных нет:

```powershell
# Остановить проект и удалить тома (полная очистка):
docker compose down -v

# Запустить заново:
docker compose up -d
```

Ждём ~2 минуты пока broker и kafka-connect поднимутся. **Важно**: после `down -v` внутренние Kafka-топики создаются с неправильным `cleanup.policy=delete`. Исправляем:

```powershell
# Ждём broker (проверяем):
docker exec broker kafka-topics --list --bootstrap-server broker:29092

# Исправляем cleanup.policy для внутренних топиков Kafka Connect:
docker exec broker kafka-configs --bootstrap-server broker:29092 --entity-type topics --entity-name _connect-offsets --alter --add-config cleanup.policy=compact
docker exec broker kafka-configs --bootstrap-server broker:29092 --entity-type topics --entity-name _connect-configs --alter --add-config cleanup.policy=compact
docker exec broker kafka-configs --bootstrap-server broker:29092 --entity-type topics --entity-name _connect-status --alter --add-config cleanup.policy=compact

# Перезапускаем kafka-connect (после исправления):
docker compose restart kafka-connect
```

Ждём ~2 минуты. Проверяем что PostgreSQL пуст:

```powershell
docker exec postgres psql -U postgres -d university -c "SELECT count(*) FROM university"
# Ожидание: 0 (нет данных)
```

---

## ШАГ 2. Генерация данных (2 университета)

Генератор заполняет **ТОЛЬКО PostgreSQL**. Остальные БД получат данные через CDC.

### 2.1 Регистрация коннекторов

Ждём пока `kafka-connect` станет healthy (~90 сек):

```powershell
curl.exe -s http://localhost:8083/ | Select-String "version"
# Должен ответить — значит Kafka Connect запущен
```

UNIQUE-constraints в Neo4j (предотвращают дубликаты при concurrent MERGE):

```powershell
docker exec neo4j cypher-shell -u neo4j -p password12345 "CREATE CONSTRAINT IF NOT EXISTS FOR (n:University) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:Institute) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:Department) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:Speciality) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:LectureCourse) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:Lecture) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:StudentGroup) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:Student) REQUIRE n.id IS UNIQUE; CREATE CONSTRAINT IF NOT EXISTS FOR (n:Schedule) REQUIRE n.id IS UNIQUE"
```

Регистрация 6 коннекторов:

```powershell
python scripts\register-connectors.py
# [1/6] debezium-postgres-source OK
# [2/6] elasticsearch-sink OK
# ...
# Waiting 30 seconds...
# debezium-postgres-source: RUNNING
# ...
```

Проверяем все 6 коннекторов RUNNING:

```powershell
curl.exe -s http://localhost:8083/connectors
# ["debezium-postgres-source","elasticsearch-sink","redis-sink","neo4j-sink","mongodb-sink-flat","mongodb-sink-hierarchy"]
```

```powershell
curl.exe -s http://localhost:8083/connectors/debezium-postgres-source/status | python -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'], d['tasks'][0]['state'])"
# RUNNING RUNNING
```

### 2.2 Запуск генерации

```powershell
curl.exe -s -X POST http://localhost:8010/generate
```

Ждём ~30 секунд. Затем пересоздаём ES-индекс для ЛР1 (при старте PG был пуст):

```powershell
docker exec lab1 python -c "from app import ensure_lectures_index; ensure_lectures_index()"
# Пересоздаёт индекс 'lectures' из PG (~1200 лекций)
```

Ждём ~30 секунд пока Debezium снимет snapshot и разнесёт по всем БД.

**Важно**: `snapshot.mode=always` в debezium-postgres-source — Debezium ВСЕГДА делает snapshot при старте коннектора (не только первый раз). Таблица attendance (120000+ записей) обрабатывается медленно через ElasticsearchCdcHandler (~10-20 мин). Можно продолжать защиту не дожидаясь полного индекса attendance.

Если MongoDB иерархия пуста (CdcHandler не успел) — триггерим UPDATE:

```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE university SET name=name; UPDATE institute SET name=name; UPDATE department SET name=name; UPDATE speciality SET name=name; UPDATE department_specialities SET is_primary=is_primary"
```

---

## ШАГ 3. Показать что данные создались во всех БД

### 3.1 PostgreSQL — 12 таблиц заполнены

```powershell
docker exec postgres psql -U postgres -d university -c "\dt"
# 12 таблиц: university, institute, department, speciality, department_specialities, lecture_course, lecture, lecture_material, student_group, student, schedule, attendance
```

```powershell
docker exec postgres psql -U postgres -d university -c "SELECT count(*) FROM university"
# 2 (два университета)

docker exec postgres psql -U postgres -d university -c "SELECT id, name, short_name FROM university"
# uuid...|РТУ МИРЭА|...
# uuid...|МГТУ им. Баумана|...

docker exec postgres psql -U postgres -d university -c "SELECT count(*) FROM student"
# ~3000 студентов
```

### 3.2 ElasticSearch — 12 индексов university.public.*

```powershell
curl.exe -s http://localhost:9200/_cat/indices?v | Select-String "university.public."
# 12 индексов: university.public.university, university.public.institute, university.public.department, university.public.speciality, university.public.department_specialities, university.public.lecture_course, university.public.lecture, university.public.lecture_material, university.public.student_group, university.public.student, university.public.schedule, university.public.attendance
```

```powershell
curl.exe -s http://localhost:9200/university.public.student/_count
# {"count":3001,...}
```

### 3.3 Redis — HASH-ключи student:* и student_group:*

```powershell
docker exec redis redis-cli KEYS "student:*" | Measure-Object -Line
# ~3000+ ключей

docker exec redis redis-cli KEYS "student_group:*" | Measure-Object -Line
# ~100+ ключей
```

```powershell
# Показать формат HASH (не string!):
$key = (docker exec redis redis-cli KEYS "student:*")[0]
docker exec redis redis-cli HGETALL $key
# Поля: id, first_name, last_name, patronymic, email, phone, student_card_number, status, enrollment_date, group_id
```

### 3.4 Neo4j — узлы и связи графа

```powershell
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY label"
# University: 2, Institute: 5, Department: 15, Speciality: 30, LectureCourse: 120, Lecture: 1200, StudentGroup: 100, Student: 3000, Schedule: 3990
# НЕТ LectureMaterial (11 топиков, не 12)
```

```powershell
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS cnt ORDER BY rel"
# ATTENDED, BELONGS_TO, CONTAINS, FOR_SPECIALITY, MEMBER_OF, PART_OF, SHOULD_ATTEND
```

```powershell
# Показать связь с is_primary:
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (sp:Speciality)-[r:PART_OF]->(d:Department) WHERE r.is_primary = true RETURN sp.name, d.name, r.is_primary LIMIT 5"
# Специальности с is_primary=true (для ЛР3)
```

### 3.5 MongoDB — flat (ExtractNewRecordState) + hierarchy (CdcHandler)

```powershell
# Flat — плоские документы всех таблиц (upsert по ключу):
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.countDocuments()"
# ~10000+ документов (каждая строка каждой таблицы = 1 документ, с полями __op и __table)
```

```powershell
# Hierarchy — вложенные документы University→Institutes→Departments→Specialities:
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university').hierarchy.countDocuments()"
# 2 (по числу университетов)

docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne(); print('name:', d.name); print('institutes:', Object.keys(d.institutes).length); var i0=Object.values(d.institutes)[0]; print('first_institute:', i0.name); print('departments:', Object.keys(i0.departments).length)"
# name: РТУ МИРЭА, institutes: N, first_institute: ..., departments: N
```

### 3.6 Kafka — CDC демо в Control Center (INSERT/UPDATE/DELETE в реальном времени)

**Открой в браузере**: http://localhost:9021 → Topics → university.public.institute → Messages

**Оставь вкладку открытой** и запусти демо-скрипт (с паузами между операциями):

```powershell
python scripts\cdc-demo.py
```

Скрипт делает:
1. **INSERT** → в UI появится сообщение `op=c` (create) с `name=DEMO_INSERT`
2. **UPDATE** → появится `op=u` (update) с `name=DEMO_UPDATED`
3. **DELETE** → появится `op=d` (delete) — tombstone-сообщение

Каждая операция с паузой 10 сек — успеешь увидеть в UI.

Также можно показать сообщения вручную:
```powershell
# CLI — 3 сообщения из топика student (из истории):
docker exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic university.public.student --from-beginning --max-messages 3
```

---

## ШАГ 4. Изменение данных → показать CDC в MongoDB

### 4.1 INSERT — новый институт в иерархии

```powershell
$univId = (docker exec postgres psql -U postgres -d university -t -A -c "SELECT id FROM university LIMIT 1")
docker exec postgres psql -U postgres -d university -c "INSERT INTO institute (id, university_id, name, short_name, dean) VALUES ('bbbb1111-0000-0000-0000-000000000001', '$univId', 'TEST_CDC_INST', 'TCI', 'Test Dean')"
Start-Sleep 12
```

Проверяем в MongoDB — иерархия обновилась:

```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne({_id:'$univId'}); print(d.institutes['bbbb1111-0000-0000-0000-000000000001'] !== undefined ? 'FOUND: ' + d.institutes['bbbb1111-0000-0000-0000-000000000001'].name : 'NOT_FOUND')"
# FOUND: TEST_CDC_INST
```

И в flat:

```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.findOne({short_name:'TCI'}, {name:1, short_name:1, _id:0})"
# { name: 'TEST_CDC_INST', short_name: 'TCI' }
```

### 4.2 UPDATE — меняем имя института

```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE institute SET name='CDC_UPDATED' WHERE short_name='TCI'"
Start-Sleep 12
```

Проверяем в MongoDB иерархии:

```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne(); print(d.institutes['bbbb1111-0000-0000-0000-000000000001'].name)"
# CDC_UPDATED
```

И в flat:

```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.findOne({short_name:'TCI'}, {name:1, _id:0})"
# { name: 'CDC_UPDATED' }
```

### 4.3 DELETE — удаляем институт из иерархии

```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM institute WHERE short_name='TCI'"
Start-Sleep 12
```

```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne(); print(d.institutes['bbbb1111-0000-0000-0000-000000000001'] !== undefined ? 'STILL EXISTS' : 'DELETED')"
# DELETED
```

```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.findOne({short_name:'TCI'})"
# null (удалён из flat тоже)
```

---

## ШАГ 5. Изменение связей в Neo4j → перепривязка

### 5.1 Показать текущую связь студента с группой

```powershell
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (s:Student)-[:MEMBER_OF]->(g:StudentGroup) RETURN s.first_name, s.last_name, g.name LIMIT 3"
# Студент → Группа
```

### 5.2 Перевод студента в другую группу (UPDATE в PG)

```powershell
# Запоминаем текущую группу студента:
$studentId = (docker exec postgres psql -U postgres -d university -t -A -c "SELECT id FROM student LIMIT 1")
$oldGroup = (docker exec postgres psql -U postgres -d university -t -A -c "SELECT group_id FROM student WHERE id='$studentId'")
$newGroup = (docker exec postgres psql -U postgres -d university -t -A -c "SELECT id FROM student_group WHERE id != '$oldGroup' LIMIT 1")

# Меняем группу:
docker exec postgres psql -U postgres -d university -c "UPDATE student SET group_id='$newGroup' WHERE id='$studentId'"
Start-Sleep 10
```

Проверяем в Neo4j — студент перепривязан:

```powershell
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (s:Student {id: '$studentId'})-[:MEMBER_OF]->(g:StudentGroup) RETURN g.name"
# Новая группа (не старая!)
```

### 5.3 Изменение is_primary на связи Speciality→Department

```powershell
# Показать связь с is_primary=false:
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (sp:Speciality)-[r:PART_OF]->(d:Department) WHERE r.is_primary = false RETURN sp.name, d.name, r.is_primary LIMIT 3"
```

```powershell
# Меняем is_primary в PostgreSQL:
docker exec postgres psql -U postgres -d university -c "UPDATE department_specialities SET is_primary = true WHERE is_primary = false LIMIT 1"
Start-Sleep 10

# Проверяем в Neo4j:
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (sp:Speciality)-[r:PART_OF]->(d:Department) WHERE r.is_primary = true RETURN count(r)"
# Количество увеличилось
```

---

## ШАГ 6. Изменение значений в ElasticSearch и Redis

### 6.1 ElasticSearch — INSERT / UPDATE / DELETE (через ElasticsearchCdcHandler)

**ElasticsearchCdcHandler** — кастомный Kafka Connect Transformation (Java), обрабатывает ВСЕ операции:
- INSERT/UPDATE: HTTP PUT с UUID как `_id` (извлекает Struct-поля из record.value())
- DELETE: HTTP DELETE при tombstone (value=null)
- Всегда возвращает null → коннектор сам НЕ пишет в ES

**INSERT** — новая кафедра появляется в ES:

```powershell
$instId = (docker exec postgres psql -U postgres -d university -t -A -c "SELECT id FROM institute LIMIT 1")
docker exec postgres psql -U postgres -d university -c "INSERT INTO department (id, name, short_name, institute_id, head) VALUES ('cccc1111-0000-0000-0000-000000000001', 'ES_TEST_DEPT', 'ETD', '$instId', 'Test Head')"
Start-Sleep 10
curl.exe -s "http://localhost:9200/university.public.department/_search?q=short_name:ETD" | python -c "import sys,json; d=json.load(sys.stdin); print('hits:', d['hits']['total']['value'])"
# hits: 1
```

**UPDATE** — меняем имя в PG → обновляется в ES:

```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE department SET name='ES_UPDATED' WHERE short_name='ETD'"
Start-Sleep 10
curl.exe -s "http://localhost:9200/university.public.department/_search?q=short_name:ETD" | python -c "import sys,json; d=json.load(sys.stdin); print('name:', d['hits']['hits'][0]['_source']['name'])"
# name: ES_UPDATED
```

**DELETE** — удаляем из PG → документ удаляется из ES:

```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM department WHERE short_name='ETD'"
Start-Sleep 10
curl.exe -s "http://localhost:9200/university.public.department/_search?q=short_name:ETD" | python -c "import sys,json; d=json.load(sys.stdin); print('hits:', d['hits']['total']['value'])"
# hits: 0 (документ удалён через ElasticsearchCdcHandler HTTP DELETE)
```

### 6.2 Redis — INSERT / UPDATE / DELETE (HSET hash)

**INSERT** — новая группа → hash в Redis:

```powershell
$specId = (docker exec postgres psql -U postgres -d university -t -A -c "SELECT id FROM speciality LIMIT 1")
docker exec postgres psql -U postgres -d university -c "INSERT INTO student_group (id, name, speciality_id, enrollment_year, curator) VALUES ('f1111111-0000-0000-0000-000000000001', 'REDIS_TEST_GRP', '$specId', 2025, 'Test Curator')"
Start-Sleep 10
docker exec redis redis-cli HGETALL "student_group:f1111111-0000-0000-0000-000000000001"
# Поля hash: id, name, speciality_id, enrollment_year, curator (name = "REDIS_TEST_GRP")
```

**UPDATE** — меняем имя группы в PG → hash обновляется в Redis:

```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE student_group SET name='REDIS_UPDATED' WHERE id='f1111111-0000-0000-0000-000000000001'"
Start-Sleep 10
docker exec redis redis-cli HGET "student_group:f1111111-0000-0000-0000-000000000001" name
# "REDIS_UPDATED"
```

**DELETE** — удаляем из PG → ключ ФИЗИЧЕСКИ удаляется из Redis:

```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM student_group WHERE id='f1111111-0000-0000-0000-000000000001'"
Start-Sleep 15
docker exec redis redis-cli EXISTS "student_group:f1111111-0000-0000-0000-000000000001"
# 0 (ключ удалён через RedisHashCdcHandler)
```

---

## ШАГ 7. Проверка что Лабы 1-3 работают корректно

### 7.1 Получить JWT-токен

```powershell
$token = (curl.exe -s -X POST http://localhost:8000/auth/login -H "Content-Type: application/json" -d '{"username":"admin","password":"admin123"}' | ConvertFrom-Json).access_token
```

### 7.2 ЛР1 — Посещаемость по термину (ES→Neo4j→PG→Redis)

```powershell
# Важно: кириллицу в URL нужно кодировать через [uri]::EscapeDataString()
$term = [uri]::EscapeDataString("вероятностей")
curl.exe -s "http://localhost:8000/attendance/low?term=$term&start_date=2025-09-01&end_date=2026-06-01" -H "Authorization: Bearer $token"
# JSON с результатами: студент, группа, % посещения
```

UI: https://localhost/api (в браузере с клиентским сертификатом) → вкладка "ЛР1"

### 7.3 ЛР2 — Нагрузка аудиторий (Neo4j)

```powershell
curl.exe -s "http://localhost:8000/schedule/capacity?semester=1&year=2025&equipment=" -H "Authorization: Bearer $token"
# JSON: курс, лекция, кол-во слушателей
```

### 7.4 ЛР3 — Отчёт по группе (Neo4j→PG, is_primary)

```powershell
curl.exe -s "http://localhost:8000/hours/report?group_name=Группа-001-2025" -H "Authorization: Bearer $token"
# Примечание: имя группы зависит от генерации; посмотреть реальные имена:
# docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (g:StudentGroup) RETURN g.name LIMIT 5"
# JSON: студент, курс, запланированные/посещённые часы
```

---

## ШАГ 8. Мониторинг

### 8.1 Telegraf → InfluxDB → Grafana

```powershell
# Telegraf работает (читает из Kafka):
docker logs telegraf --tail 5
# Должны быть строки с "metrics=..."

# InfluxDB healthy:
curl.exe -s http://localhost:8086/health
# {"status":"pass"}

# Grafana healthy:
curl.exe -s http://localhost:3000/api/health
# {"database":"ok","commit":"..."}
```

### 8.2 Grafana UI

```
http://localhost:3000
Логин: admin / Пароль: admin
(при первом входе предложит сменить пароль — можно Skip)
```

В Grafana:
- **Data Source**: InfluxDB (`http://influxdb:8086`, bucket `kafka`, org `papo`, token из docker-compose)
- **Dashboard**: создать с метриками Kafka (messages in/out, consumer lag)

### 8.3 Control Center (Kafka UI)

```
http://localhost:9021
```

Показать:
- **Topics** → 12 топиков `university.public.*` → Messages (видно JSON записей)
- **Connectors** → 6 коннекторов RUNNING
- **Consumer groups** → lag = 0 (всё прочитано)

### 8.4 Kafka Connect — все коннекторы RUNNING

```powershell
curl.exe -s http://localhost:8083/connectors | python -c "import sys,json; [print(c) for c in json.load(sys.stdin)]"
# debezium-postgres-source
# elasticsearch-sink
# redis-sink
# neo4j-sink
# mongodb-sink-flat
# mongodb-sink-hierarchy

curl.exe -s http://localhost:8083/connectors/debezium-postgres-source/status | python -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])"
# RUNNING (и так для каждого)
```

---

## СВОДКА: что показывать преподавателю

| # | Что показать | Ожидаемый результат |
|---|-------------|-------------------|
| 1 | Пустой PG до генерации | `SELECT count(*) FROM university` → 0 |
| 2 | Генерация данных | `POST /generate` → 2 университета, ~3000 студентов |
| 3 | PG заполнен | 12 таблиц, `SELECT count(*) FROM university` → 2 |
| 4 | ES заполнен через CDC | 12 индексов `university.public.*`, `university.public.student/_count` → 3001 |
| 5 | Redis заполнен через CDC | `KEYS student:*` → ~3000, тип HASH (HGETALL) |
| 6 | Neo4j заполнен через CDC | Узлы + связи, `is_primary` на PART_OF |
| 7 | MongoDB flat через CDC | ~10000 документов в `flat_data` (upsert по ключу, с __op/__table) |
| 8 | MongoDB hierarchy (CdcHandler) | 2 вложенных документа в `hierarchy` |
| 9 | INSERT в PG → MongoDB обновляется | Новый институт в иерархии + flat |
| 10 | UPDATE в PG → MongoDB обновляется | Имя изменилось в иерархии + flat |
| 11 | DELETE из PG → MongoDB удаляется | Институт удалён из иерархии + flat |
| 12 | Neo4j: студент переведён в другую группу | MEMBERS_OF перепривязался |
| 13 | Neo4j: is_primary изменён | PART_OF.is_primary обновился |
| 14 | ES: INSERT/UPDATE/DELETE | Документы создаются, обновляются, удаляются |
| 15 | Redis: INSERT/UPDATE/DELETE | HSET создаётся, обновляется, ключ DEL удаляется |
| 16 | ЛР1 работает | Запрос по термину → JSON результат |
| 17 | ЛР2 работает | Запрос по семестру → JSON результат |
| 18 | ЛР3 работает | Запрос по группе → JSON с is_primary |
| 19 | Мониторинг: Grafana | http://localhost:3000 — дашборд с метриками |
| 20 | Мониторинг: Control Center | http://localhost:9021 — топики, коннекторы |
| 21 | Мониторинг: InfluxDB healthy | http://localhost:8086/health → pass |

---

## Полезные шорткаты

```powershell
# Все контейнеры:
docker ps --format "table {{.Names}}\t{{.Status}}"

# Все коннекторы:
curl.exe -s http://localhost:8083/connectors

# Kafka топики:
docker exec broker kafka-topics --list --bootstrap-server broker:29092 | Select-String "university"

# PG → 12 таблиц:
docker exec postgres psql -U postgres -d university -c "\dt"

# ES → все индексы:
curl.exe -s http://localhost:9200/_cat/indices?v | Select-String "university.public."

# Redis → ключи:
docker exec redis redis-cli KEYS "student:*" | Measure-Object -Line

# Neo4j → узлы:
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (n) RETURN labels(n)[0], count(n)"

# MongoDB → flat:
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.countDocuments()"

# MongoDB → hierarchy:
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university').hierarchy.findOne()"
```
