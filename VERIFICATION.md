# Команды для проверки работоспособности (защита лаб 4-5)

Все команды выполняются в **PowerShell** (Windows).
Комментарии после `#` — что должно получиться.

---

## 0. Запуск проекта

```powershell
# Из корня проекта (папка lab/):
docker compose up -d
```

Ждать ~2 минуты пока все контейнеры станут healthy.

Регистрация коннекторов (выполнять ПОСЛЕ того как kafka-connect стал healthy):
```powershell
python -c "
import json, urllib.request, os, time
url = 'http://localhost:8083/connectors'
dir_path = os.path.join('scripts', 'connectors')
connectors = [
    ('debezium-postgres-source', 'debezium-postgres-source.json'),
    ('elasticsearch-sink',       'elasticsearch-sink.json'),
    ('redis-sink',               'redis-sink.json'),
    ('neo4j-sink',               'neo4j-sink.json'),
    ('mongodb-sink-flat',        'mongodb-sink-flat.json'),
    ('mongodb-sink-hierarchy',   'mongodb-sink-hierarchy.json'),
]
for i, (name, fname) in enumerate(connectors, 1):
    with open(os.path.join(dir_path, fname), encoding='utf-8') as f:
        config = json.load(f)
    body = json.dumps({'name': name, 'config': config})
    req = urllib.request.Request(url, data=body.encode(), headers={'Content-Type':'application/json'})
    try:
        urllib.request.urlopen(req); print('[%d/6] %s OK' % (i, name))
    except urllib.error.HTTPError as e:
        print('[%d/6] %s ERROR: %s' % (i, name, json.loads(e.read()).get('message','')[:120]))
time.sleep(30)
for name, _ in connectors:
    try:
        st = json.loads(urllib.request.urlopen(url+'/'+name+'/status').read())
        print('%s: %s' % (name, st['connector']['state']))
    except: print('%s: FAILED' % name)
"
```

Проверить что все 6 коннекторов RUNNING:
```powershell
curl.exe -s http://localhost:8083/connectors
# Должно быть: ["redis-sink","mongodb-sink-hierarchy","debezium-postgres-source","neo4j-sink","elasticsearch-sink","mongodb-sink-flat"]

curl.exe -s http://localhost:8083/connectors/debezium-postgres-source/status | python -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])"
# Должно быть: RUNNING
```

Если MongoDB иерархия пуста — триггерим UPDATE на 5 таблиц:
```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE university SET name=name; UPDATE institute SET name=name; UPDATE department SET name=name; UPDATE speciality SET name=name; UPDATE department_specialities SET is_primary=is_primary"
```

Генерация данных (если PostgreSQL пуст):
```powershell
curl.exe -s -X POST http://localhost:8010/generate
# Генератор заполняет ТОЛЬКО PostgreSQL, остальные БД заполняются через CDC
```

---

## 1. Все контейнеры работают

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Должно быть **19 контейнеров**, все Up/Healthy:
zookeeper, broker, schema-registry, kafka-connect, control-center,
postgres, generator, elasticsearch, redis, neo4j, mongodb,
telegraf, influxdb, grafana, lab1, lab2, lab3, nginx, api-gateway
(redis-cdc-delete удалён — Delete теперь через DeletingCdcHandler SMT)

---

## 2. PostgreSQL — CDC настройка (Лаб 4, шаг 1)

### 2.1 wal2json модуль установлен
```powershell
docker exec postgres bash -c "dpkg -l 2>/dev/null | grep wal2json"
```
Должно быть: `ii  postgresql-16-wal2json  ...  PostgreSQL logical decoding JSON output plugin`

### 2.2 Схема предыдущего семестра (12 таблиц)
```powershell
docker exec postgres psql -U postgres -d university -c "\dt"
```
Должно быть 12 таблиц: university, institute, department, speciality, department_specialities, lecture_course, lecture, lecture_material, student_group, student, schedule, attendance (+ 4 партиции attendance_*)

### 2.3 PUBLICATION pub для всех таблиц
```powershell
docker exec postgres psql -U postgres -d university -c "SELECT * FROM university.public.publication"
```
Должно быть: pubname=**pub**, puballtables=**t** (все таблицы)

### 2.4 wal_level = logical (из postgresql.conf)
```powershell
docker exec postgres psql -U postgres -d university -c "SHOW wal_level"
```
Должно быть: **logical**

### 2.5 Конфигурация postgresql.conf
```powershell
type postgres\postgresql.conf
```
Должно быть: wal_level = logical, max_wal_senders = 4, max_replication_slots = 4, wal_keep_size = 256MB

---

## 3. Kafka (Лаб 4, шаг 2)

### 3.1 Компоненты: Broker + Zookeeper + Schema Registry + Kafka Connect + Control Center
```powershell
docker ps --format "{{.Names}} {{.Status}}" | Select-String "zookeeper|broker|schema-registry|kafka-connect|control-center"
```
Все 5 контейнеров Up

### 3.2 Docker Compose очищен — только нужные компоненты
В docker-compose.yml **НЕТ**: ksqldb, mysql, kibana, kafkacat (из примера методички)

### 3.3 Debezium Source Connector — RUNNING
```powershell
curl.exe -s http://localhost:8083/connectors/debezium-postgres-source/status | python -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'], d['tasks'][0]['state'])"
```
Должно быть: **RUNNING RUNNING**

### 3.4 Конфиг Debezium: Initial Snapshot, pgoutput, publication=pub
```powershell
curl.exe -s http://localhost:8083/connectors/debezium-postgres-source/config | python -c "import sys,json; d=json.load(sys.stdin); print('snapshot.mode:', d.get('snapshot.mode')); print('plugin.name:', d.get('plugin.name')); print('publication.name:', d.get('publication.name'))"
```
Должно быть: snapshot.mode=**initial**, plugin.name=**pgoutput**, publication.name=**pub**

### 3.5 12 топиков (1 топик на таблицу)
```powershell
docker exec broker kafka-topics --list --bootstrap-server broker:29092 | Select-String "university.public"
```
Должно быть 12 топиков: university.public.university, .institute, .department, .speciality, .department_specialities, .lecture_course, .lecture, .lecture_material, .student_group, .student, .schedule, .attendance

### 3.6 Проверка данных в Control Center
```
Браузер: http://localhost:9021
Раздел Topics → university.public.student → Messages
```

---

## 4. ElasticSearch Sink (Лаб 4, шаг 3)

### 4.1 Docker Compose секция для ES — ЗАВОДСКОЙ контейнер
```powershell
docker inspect elasticsearch --format "{{.Config.Image}}"
```
Должно быть: docker.elastic.co/elasticsearch/elasticsearch:**8.12.0** (стандартный образ, не кастомный)

### 4.2 ES Sink Connector — ВСЕ 12 топиков + ElasticsearchCdcHandler для DELETE
```powershell
curl.exe -s http://localhost:8083/connectors/elasticsearch-sink/config | python -c "import sys,json; d=json.load(sys.stdin); topics=d.get('topics','').split(','); print(len(topics),'topics'); print('cdcDelete transform:', d.get('transforms.cdcDelete.type','').split('.')[-1])"
```
Должно быть: **12 topics**, cdcDelete transform=**ElasticsearchCdcHandler** (Lenses elastic7 не имеет behavior.on.null.values, DELETE реализован через кастомный Transform)

### 4.3 12 индексов university.public.* в ElasticSearch
```powershell
curl.exe -s http://localhost:9200/_cat/indices?v | Select-String "university.public."
```
12 индексов: university.public.university, university.public.institute, university.public.department, university.public.speciality, university.public.department_specialities, university.public.lecture_course, university.public.lecture, university.public.lecture_material, university.public.student_group, university.public.student, university.public.schedule, university.public.attendance

### 4.4 CRUD — Create (INSERT в Postgres → появляется в ES)
```powershell
# Вставляем тестовую запись в PostgreSQL:
docker exec postgres psql -U postgres -d university -c "INSERT INTO institute (id, name, short_name, university_id) VALUES ('a1111111-0000-0000-0000-000000000001', 'ES_TEST_INST', 'ETI', (SELECT id FROM university LIMIT 1))"
# Ждём 10 сек пока CDC дойдет:
Start-Sleep 10
# Проверяем в ES:
curl.exe -s "http://localhost:9200/university.public.institute/_search?q=short_name:ETI" | python -c "import sys,json; d=json.load(sys.stdin); print('hits:', d['hits']['total']['value'])"
```
Должно быть: hits: **1**

### 4.5 CRUD — Update (UPDATE в PG → обновляется в ES)
```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE institute SET name='ES_UPDATED' WHERE short_name='ETI'"
Start-Sleep 10
curl.exe -s "http://localhost:9200/university.public.institute/_search?q=short_name:ETI" | python -c "import sys,json; d=json.load(sys.stdin); h=d['hits']['hits']; print('name:', h[0]['_source']['name'] if len(h)>0 else 'NOT FOUND')"
```
Должно быть: name: **ES_UPDATED**

### 4.6 CRUD — Delete (DELETE из PG → документ удаляется из ES)
```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM institute WHERE short_name='ETI'"
Start-Sleep 10
curl.exe -s "http://localhost:9200/university.public.institute/_search?q=short_name:ETI" | python -c "import sys,json; d=json.load(sys.stdin); print('hits:', d['hits']['total']['value'])"
```
Должно быть: hits: **0** (документ физически удалён)

---

## 5. Redis Sink (Лаб 4, шаг 4)

### 5.1 Docker Compose секция для Redis — ЗАВОДСКОЙ контейнер
```powershell
docker inspect redis --format "{{.Config.Image}}"
```
Должно быть: redis:**7-alpine** (стандартный образ, не кастомный)

### 5.2 Redis Sink Connector (stream-reactor + RedisHashCdcHandler SMT) — ТОЛЬКО кешируемые таблицы
```powershell
curl.exe -s http://localhost:8083/connectors/redis-sink/config | python -c "import sys,json; d=json.load(sys.stdin); print('connector.class:', d.get('connector.class','').split('.')[-1]); print('topics:', d.get('topics')); print('transform:', d.get('transforms.redisHash.type','').split('.')[-1])"
```
Должно быть: connector.class=**RedisSinkConnector**, topics=**university.public.student,university.public.student_group** (только 2 кешируемые таблицы), transform=**RedisHashCdcHandler**

### 5.3 Данные в Redis Hash (только student + student_group)
```powershell
docker exec redis redis-cli KEYS "student:*" | Measure-Object -Line      # ~2000+ ключей
docker exec redis redis-cli KEYS "student_group:*" | Measure-Object -Line # ~100+ ключей
docker exec redis redis-cli KEYS "*" | Measure-Object -Line               # только student:* + student_group:*
```
НЕ должно быть ключей других таблиц — только student:* и student_group:*

### 5.4 Формат данных в Redis — HASH (UPSERT INTO student: = HSET)
```powershell
# Получить первый попавшийся ключ студента:
$key = (docker exec redis redis-cli KEYS "student:*")[0]
docker exec redis redis-cli HGETALL $key
```
Должны быть поля хэша: id, first_name, last_name, patronymic, email, phone, student_card_number, status, enrollment_date, group_id

### 5.5 CRUD — Create (INSERT в PG → hash появляется в Redis)
```powershell
docker exec postgres psql -U postgres -d university -c "INSERT INTO student_group (id, name, speciality_id, enrollment_year, curator) VALUES ('f1111111-0000-0000-0000-000000000001', 'REDIS_TEST_GRP', (SELECT id FROM speciality LIMIT 1), 2025, 'Test Curator')"
Start-Sleep 10
docker exec redis redis-cli HGETALL "student_group:f1111111-0000-0000-0000-000000000001"
```
Должен вернуться hash с полем name=**"REDIS_TEST_GRP"**

### 5.6 CRUD — Update (UPDATE в PG → hash обновляется в Redis)
```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE student_group SET name='REDIS_UPDATED' WHERE id='f1111111-0000-0000-0000-000000000001'"
Start-Sleep 10
docker exec redis redis-cli HGET "student_group:f1111111-0000-0000-0000-000000000001" name
```
Должно быть: **"REDIS_UPDATED"**

### 5.7 CRUD — Delete (ключ ФИЗИЧЕСКИ удаляется из Redis через RedisHashCdcHandler)
**Важно**: stream-reactor не умеет физически удалять ключи и не поддерживает HSET.
Для HSET + физического Delete добавлен кастомный Kafka Connect Transformation **RedisHashCdcHandler**
(Java-класс внутри redis-cdc-handler.jar): при получении tombstone (value=null) он делает `jedis.del()` в Redis,
при обычной записи — `jedis.hset()` (HSET hash). Transform возвращает null, чтобы stream-reactor не дублировал.

```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM student_group WHERE id='f1111111-0000-0000-0000-000000000001'"
Start-Sleep 15
docker exec redis redis-cli EXISTS "student_group:f1111111-0000-0000-0000-000000000001"
```
Должно быть: **0** (ключ ФИЗИЧЕСКИ удалён)

---

## 6. Neo4j Sink (Лаб 5, шаг 1)

### 6.1 Docker Compose секция для Neo4j — ЗАВОДСКОЙ контейнер
```powershell
docker inspect neo4j --format "{{.Config.Image}}"
```
Должно быть: neo4j:**5** (стандартный образ, не кастомный)

### 6.2 Neo4j Sink Connector — ТОЛЬКО таблицы графовых связей (11, БЕЗ lecture_material)
```powershell
curl.exe -s http://localhost:8083/connectors/neo4j-sink/config | python -c "import sys,json; d=json.load(sys.stdin); topics=d.get('topics','').split(','); print(len(topics),'topics'); print('has lecture_material:','lecture_material' in d.get('topics',''))"
```
Должно быть: **11 topics**, has lecture_material: **False**

### 6.3 Узлы в Neo4j
```powershell
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY label"
```
Должны быть типы: University, Institute, Department, Speciality, LectureCourse, Lecture, StudentGroup, Student, Schedule
**НЕ должно быть** типа LectureMaterial

### 6.4 Связи между узлами
```powershell
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS cnt ORDER BY rel"
```
Должны быть связи: PART_OF, MEMBER_OF, BELONGS_TO, FOR_SPECIALITY, CONTAINS, SHOULD_ATTEND, ATTENDED

### 6.5 CRUD — Create (INSERT в PG → узел появляется в Neo4j)
```powershell
docker exec postgres psql -U postgres -d university -c "INSERT INTO speciality (id, name, code, degree_level, duration_years) VALUES ('c1111111-0000-0000-0000-000000000001', 'NEO_TEST_SPEC', '99.01.01', 'test', 4)"
Start-Sleep 10
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (s:Speciality {name:'NEO_TEST_SPEC'}) RETURN s.name, s.code"
```
Должно быть: "NEO_TEST_SPEC", "99.01.01"

### 6.6 CRUD — Update (UPDATE в PG → свойства узла обновляются)
```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE speciality SET name='NEO_UPDATED' WHERE code='99.01.01'"
Start-Sleep 10
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (s:Speciality {code:'99.01.01'}) RETURN s.name"
```
Должно быть: **"NEO_UPDATED"**

### 6.7 CRUD — Delete (DELETE из PG → узел ФИЗИЧЕСКИ удаляется из Neo4j с DETACH DELETE)
```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM speciality WHERE code='99.01.01'"
Start-Sleep 10
docker exec neo4j cypher-shell -u neo4j -p password12345 "MATCH (s:Speciality {code:'99.01.01'}) RETURN count(s)"
```
Должно быть: **0**

---

## 7. MongoDB Sink — flat (Лаб 5, шаг 2, PostgresHandler)

### 7.1 Docker Compose секция для MongoDB — СВОЙ контейнер (не заводской!)
```powershell
type mongodb\Dockerfile
```
Должен быть: `FROM mongo:7` + `LABEL` (доказывает что контейнер свой, не заводской)
(по заданию: Redis/ES/Neo4j — заводские, а у MongoDB — свой)

### 7.2 MongoDB Sink Connector (flat) — ВСЕ 12 топиков, встроенный PostgresHandler
```powershell
curl.exe -s http://localhost:8083/connectors/mongodb-sink-flat/config | python -c "import sys,json; d=json.load(sys.stdin); print('handler:', d.get('change.data.capture.handler','').split('.')[-1]); print('database:', d.get('database')); print('collection:', d.get('collection')); print('delete.on.null.values:', d.get('delete.on.null.values'))"
```
Должно быть: handler=**PostgresHandler**, database=**university_cdc**, collection=**flat_data**, delete.on.null.values=**true**

### 7.3 Количество документов в flat_data
```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.countDocuments()"
```
Должно быть: ~138000+ документов

### 7.4 Пример плоского документа (без Debezium envelope)
```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.findOne({name:{$exists:true}}, {_id:0, name:1, short_name:1})"
```
Должен быть документ с полями таблицы (name, short_name, ...), без "payload"/"op" полей

### 7.5 CRUD — Create
```powershell
docker exec postgres psql -U postgres -d university -c "INSERT INTO speciality (id, name, code, degree_level, duration_years) VALUES ('d1111111-0000-0000-0000-000000000001', 'MONGO_FLAT_TEST', '88.02.02', 'test2', 5)"
Start-Sleep 10
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.findOne({code:'88.02.02'}, {name:1, _id:0})"
```
Должно быть: `{ name: 'MONGO_FLAT_TEST' }`

### 7.6 CRUD — Update
```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE speciality SET name='MONGO_FLAT_UPD' WHERE code='88.02.02'"
Start-Sleep 10
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.findOne({code:'88.02.02'}, {name:1, _id:0})"
```
Должно быть: `{ name: 'MONGO_FLAT_UPD' }`

### 7.7 CRUD — Delete (документ удаляется из MongoDB)
```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM speciality WHERE code='88.02.02'"
Start-Sleep 10
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('university_cdc').flat_data.findOne({code:'88.02.02'})"
```
Должно быть: **null**

---

## 8. MongoDB Sink — hierarchy (КАСТОМНЫЙ CdcHandler, Лаб 5 шаг 5)

### 8.1 Конфигурация: использует наш Java-класс (НЕ встроенный PostgresHandler)
```powershell
curl.exe -s http://localhost:8083/connectors/mongodb-sink-hierarchy/config | python -c "import sys,json; d=json.load(sys.stdin); print('handler:', d.get('change.data.capture.handler')); print('topics:', d.get('topics'))"
```
Должно быть: handler=**com.example.UniversityHierarchyCdcHandler**, 5 topics (university, institute, department, speciality, department_specialities)

### 8.2 CdcHandler встроен ВНУТРЬ mongodb-connector.jar (НЕ отдельный JAR-файл)
```powershell
# На контейнере kafka-connect:
docker exec kafka-connect bash -c "jar tf /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib/mongodb-connector.jar | grep UniversityHierarchy"
```
Должно быть: `com/example/UniversityHierarchyCdcHandler.class`

**Почему отдельный JAR не работает**: MongoDB connector не видит класс из другого JAR,
даже если они лежат в одной директории. Приходится вшивать внутрь connector JAR.
(Подтверждено однокурсниками и преподавателем.)

### 8.3 Исходный код CdcHandler (extends CdcHandler)
```powershell
# Если есть исходник в репозитории:
Select-String -Path "lab-4-5\mongo-cdc-handler\src\main\java\com\example\UniversityHierarchyCdcHandler.java" -Pattern "extends CdcHandler"
```
Должно быть: `public class UniversityHierarchyCdcHandler extends CdcHandler`

### 8.4 Иерархический документ — структура University → Institutes → Departments
```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne(); print('Top-level keys: '+Object.keys(d).join(', '))"
```
Должно быть: name, short_name, address, founded_year, institutes, specialities_pool (или dept_spec_links)

### 8.5 Вложенная структура
```powershell
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne(); print('Universities: '+db.getSiblingDB('university').hierarchy.countDocuments()); var i0=Object.values(d.institutes)[0]; print('First institute: '+i0.name+', departments: '+(i0.departments?Object.keys(i0.departments).length:'0'))"
```
Должно быть: University с вложенными институтами, в каждом — кафедры

### 8.6 CRUD — Create (INSERT института → появляется в иерархии)
```powershell
$univId = (docker exec postgres psql -U postgres -d university -t -A -c "SELECT id FROM university LIMIT 1")
docker exec postgres psql -U postgres -d university -c "INSERT INTO institute (id, university_id, name, short_name, dean) VALUES ('bbbb1111-0000-0000-0000-000000000001', '$univId', 'HIER_TEST_INST', 'HTI', 'Test Dean')"
Start-Sleep 12
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne({_id:'$univId'}); print(d.institutes['bbbb1111-0000-0000-0000-000000000001'] !== undefined ? 'FOUND' : 'NOT_FOUND')"
```
Должно быть: **FOUND**

### 8.7 CRUD — Update (UPDATE института → имя меняется в иерархии)
```powershell
docker exec postgres psql -U postgres -d university -c "UPDATE institute SET name='HIER_UPDATED' WHERE short_name='HTI'"
Start-Sleep 12
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne(); var inst=d.institutes['bbbb1111-0000-0000-0000-000000000001']; print(inst.name)"
```
Должно быть: **HIER_UPDATED**

### 8.8 CRUD — Delete (DELETE института → удаляется из иерархии через $unset)
```powershell
docker exec postgres psql -U postgres -d university -c "DELETE FROM institute WHERE short_name='HTI'"
Start-Sleep 12
docker exec mongodb mongosh -u mongo -p password12345 --authenticationDatabase admin --quiet --eval "var d=db.getSiblingDB('university').hierarchy.findOne(); print(d.institutes['bbbb1111-0000-0000-0000-000000000001'] !== undefined ? 'STILL EXISTS' : 'DELETED FROM HIERARCHY')"
```
Должно быть: **DELETED FROM HIERARCHY**

---

## 9. Мониторинг: Telegraf → InfluxDB → Grafana

```powershell
docker logs telegraf --tail 3                    # Telegraf читает из Kafka
curl.exe -s http://localhost:8086/health             # InfluxDB ready → {"status":"pass"}
curl.exe -s http://localhost:3000/api/health         # Grafana → {"database":"ok"}
# Grafana UI: http://localhost:3000 (admin/admin)
# Control Center: http://localhost:9021
```

---

## 10. Контейнеры: заводские vs свои

| Контейнер | Тип | Образ | Почему |
|-----------|-----|-------|--------|
| Redis | Заводской (сингл) | redis:7-alpine | Стандартный образ |
| ElasticSearch | Заводской (сингл) | elasticsearch:8.12.0 | Стандартный образ |
| Neo4j | Заводской (сингл) | neo4j:5 | Стандартный образ |
| MongoDB | **СВОЙ** | build: ./mongodb (FROM mongo:7) | Custom CDC handler в Kafka Connect |

---

## 11. Генератор заполняет ТОЛЬКО PostgreSQL

```powershell
# Генератор НЕ подключается к другим БД:
Select-String -Path "generator\generator.py" -Pattern "from elasticsearch|from neo4j|import redis|from pymongo"
```
Должно быть: **пусто** (нет подключений к ES/Neo4j/Redis/MongoDB)

```powershell
type generator\requirements.txt
```
Должно быть: только **psycopg2-binary**, **fastapi**, **uvicorn** (нет elasticsearch, neo4j, redis, pymongo)

Все остальные БД получают данные ТОЛЬКО через CDC пайплайн:
PG → Debezium → Kafka → Sink connectors → ES/Redis/Neo4j/MongoDB

---

## 12. Проверка что Labs 1-3 работают корректно

### Контейнеры запущены
```powershell
docker ps --format "{{.Names}}" | Select-String "lab"
```
Должно быть: lab1, lab2, lab3

### Lab1: lectures ES-индекс создан из PG при старте
```powershell
curl.exe -s http://localhost:9200/lectures/_count
```
Должно быть: count > 0

### Lab1 + Lab2 + Lab3: API отвечает
```powershell
# Получить JWT-токен:
$token = (curl.exe -s -X POST http://localhost:8000/auth/login -H "Content-Type: application/json" -d '{"username":"admin","password":"admin123"}' | ConvertFrom-Json).access_token

# ЛР1 — минимальный % посещения по термину:
curl.exe -s "http://localhost:8000/attendance/low?term=programming&start_date=2025-09-01&end_date=2026-06-01" -H "Authorization: Bearer $token"

# ЛР2 — ёмкость аудитории по семестру/оборудованию:
curl.exe -s "http://localhost:8000/schedule/capacity?semester=1&year=2026&equipment=" -H "Authorization: Bearer $token"

# ЛР3 — отчёт по группе:
curl.exe -s "http://localhost:8000/hours/report?group_name=Group-001" -H "Authorization: Bearer $token"
```
Все должны вернуть JSON с результатом

---

## СВОДКА проверок для преподавателя

| # | Что показать | Команда-шорткат |
|---|-------------|----------------|
| 1 | wal2json установлен | `docker exec postgres bash -c "dpkg -l \| grep wal2json"` |
| 2 | PUBLICATION pub FOR ALL TABLES | `docker exec postgres psql -U postgres -d university -c "SELECT * FROM university.public.publication"` |
| 3 | wal_level=logical | `docker exec postgres psql -U postgres -d university -c "SHOW wal_level"` |
| 4 | 12 таблиц схемы | `docker exec postgres psql -U postgres -d university -c "\dt"` |
| 5 | 12 Kafka топиков (1 на таблицу) | `docker exec broker kafka-topics --list --bootstrap-server broker:29092 \| grep university` |
| 6 | 6 коннекторов RUNNING | `curl.exe -s http://localhost:8083/connectors` |
| 7 | ES: 12 university.public.* индексов + CRUD | `curl.exe -s http://localhost:9200/_cat/indices?v \| grep university.public.` |
| 8 | Redis: student:* + student_group:* (HSET hash) | `docker exec redis redis-cli HGETALL student:UUID` |
| 9 | Neo4j: узлы + связи + НЕТ LectureMaterial | `docker exec neo4j cypher-shell -u neo4j -p password12345 "CALL db.labels()"` |
| 10 | MongoDB flat: PostgresHandler | `curl.exe -s http://localhost:8083/connectors/mongodb-sink-flat/config` |
| 11 | MongoDB hierarchy: CdcHandler | `curl.exe -s http://localhost:8083/connectors/mongodb-sink-hierarchy/config` |
| 12 | Иерархический документ | `docker exec mongodb mongosh ... --eval "db.getSiblingDB('university').hierarchy.findOne()"` |
| 13 | CdcHandler встроен в JAR | `docker exec kafka-connect bash -c "jar tf .../mongodb-connector.jar \| grep University"` |
| 14 | MongoDB СВОЙ контейнер | `type mongodb\Dockerfile` |
| 15 | Control Center | http://localhost:9021 |
| 16 | INSERT в PG → синхронизация со ВСЕМИ БД | Команды из секций 4.4-8.6 |
| 17 | UPDATE в PG → обновление во ВСЕХ БД | Команды из секций 4.5-8.7 |
| 18 | DELETE из PG → удаление из ВСЕХ БД | Команды из секций 4.6-8.8 |
| 19 | Генератор → только PG | `type generator\requirements.txt` |
| 20 | Лабы 1-3 работают | `$token = ...; curl ... -H "Authorization: Bearer $token"` |
