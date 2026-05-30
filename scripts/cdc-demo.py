import time, subprocess, json, sys, os, urllib.request
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')

def psql(cmd):
    r = subprocess.run(['docker', 'exec', 'postgres', 'psql', '-U', 'postgres', '-d', 'university', '-c', cmd],
                       capture_output=True, text=True, encoding='utf-8')
    out = r.stdout.strip()
    for line in out.split('\n'):
        if line and not line.startswith('(0 rows)') and not line.startswith('--'):
            print(f'  PG: {line}')

def psql_val(cmd):
    r = subprocess.run(['docker', 'exec', 'postgres', 'psql', '-U', 'postgres', '-d', 'university', '-t', '-A', '-c', cmd],
                       capture_output=True, text=True, encoding='utf-8')
    return r.stdout.strip()

def neo4j(cmd):
    r = subprocess.run(['docker', 'exec', 'neo4j', 'cypher-shell', '-u', 'neo4j', '-p', 'password12345', cmd],
                       capture_output=True, text=True, encoding='utf-8')
    out = r.stdout.strip()
    if out:
        print(f'  Neo4j: {out}')

def mongo_cmd(eval_str):
    r = subprocess.run(['docker', 'exec', 'mongodb', 'mongosh', '-u', 'mongo', '-p', 'password12345',
                        '--authenticationDatabase', 'admin', '--quiet', '--eval', eval_str],
                       capture_output=True, text=True, encoding='utf-8')
    out = r.stdout.strip()
    if out and out != 'null':
        print(f'  MongoDB: {out}')
    else:
        print(f'  MongoDB: (не найдено)')

def redis_cmd(*args):
    r = subprocess.run(['docker', 'exec', 'redis', 'redis-cli'] + list(args),
                       capture_output=True, text=True, encoding='utf-8')
    out = r.stdout.strip()
    if out:
        print(f'  Redis: {out}')
    else:
        print(f'  Redis: (пусто)')

def es_get(index, query):
    try:
        url = f'http://localhost:9200/{index}/_search?q={query}'
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req)
        d = json.loads(resp.read())
        hits = d['hits']['total']['value']
        if hits > 0:
            src = d['hits']['hits'][0]['_source']
            print(f'  ES: {json.dumps(src, ensure_ascii=False)[:250]}')
        else:
            print(f'  ES: (не найдено)')
    except Exception as e:
        print(f'  ES: ошибка - {e}')

def pause(sec=20):
    print(f'\n  >>> Ждём {sec} сек пока CDC дойдёт до всех БД... <<<')
    time.sleep(sec)

def sep(title):
    print('\n' + '=' * 70)
    print(f'  {title}')
    print('=' * 70 + '\n')

# ===== Данные для демо =====
univ_id = psql_val("SELECT id FROM university LIMIT 1")
inst_id = 'bbbb1111-0000-0000-0000-000000000001'
spec_id = psql_val("SELECT id FROM speciality LIMIT 1")
grp_id  = 'f1111111-0000-0000-0000-000000000001'

print('\n' + '#' * 70)
print('  CDC ВИЗУАЛЬНОЕ ДЕМО - INSERT / UPDATE / DELETE')
print('  Kafka UI: http://localhost:9021')
print('  Открой: Topics -> university.public.institute -> Messages')
print('         Topics -> university.public.student_group -> Messages')
print('#' * 70)

# ============================================================
sep('ШАГ 1: INSERT - новые институт + группа')
# ============================================================

print('  === PostgreSQL (источник) ===')
psql(f"INSERT INTO institute (id, university_id, name, short_name, dean) VALUES ('{inst_id}', '{univ_id}', 'DEMO_INST', 'DMI', 'Demo Dean')")
psql(f"INSERT INTO student_group (id, name, speciality_id, enrollment_year, curator) VALUES ('{grp_id}', 'DEMO_GRP', '{spec_id}', 2025, 'Demo Curator')")
psql(f"UPDATE university SET name=name WHERE id='{univ_id}'")
print('\n  >>> СМОТРИ Control Center - 3 новых сообщения! <<<')
pause(20)

print('  === ElasticSearch ===')
es_get('university.public.institute', 'short_name:DMI')
es_get('university.public.student_group', 'name:DEMO_GRP')

print('\n  === Redis ===')
redis_cmd('HGETALL', f'student_group:{grp_id}')

print('\n  === Neo4j ===')
neo4j(f"MATCH (i:Institute {{id: '{inst_id}'}}) RETURN i.name")
neo4j(f"MATCH (sg:StudentGroup {{id: '{grp_id}'}}) RETURN sg.name")

print('\n  === MongoDB flat ===')
mongo_cmd(f"db.getSiblingDB('university_cdc').flat_data.findOne({{short_name:'DMI'}}, {{name:1, short_name:1, _id:0}})")
mongo_cmd(f"db.getSiblingDB('university_cdc').flat_data.findOne({{name:'DEMO_GRP'}}, {{name:1, _id:0}})")

print('\n  === MongoDB hierarchy ===')
mongo_cmd(f"var d=db.getSiblingDB('university').hierarchy.findOne(); print(d.institutes['{inst_id}'] !== undefined ? 'name: '+d.institutes['{inst_id}'].name : 'NOT_FOUND')")

# ============================================================
sep('ШАГ 2: UPDATE - меняем имена')
# ============================================================

print('  === PostgreSQL (источник) ===')
psql(f"UPDATE institute SET name='DEMO_UPDATED_INST' WHERE id='{inst_id}'")
psql(f"UPDATE student_group SET name='DEMO_UPDATED_GRP' WHERE id='{grp_id}'")
print('\n  >>> СМОТРИ Control Center - 2 сообщения op=u! <<<')
pause(15)

print('  === ElasticSearch ===')
es_get('university.public.institute', 'short_name:DMI')
es_get('university.public.student_group', 'name:DEMO_UPDATED_GRP')

print('\n  === Redis ===')
redis_cmd('HGET', f'student_group:{grp_id}', 'name')

print('\n  === Neo4j ===')
neo4j(f"MATCH (i:Institute {{id: '{inst_id}'}}) RETURN i.name")
neo4j(f"MATCH (sg:StudentGroup {{id: '{grp_id}'}}) RETURN sg.name")

print('\n  === MongoDB flat ===')
mongo_cmd(f"db.getSiblingDB('university_cdc').flat_data.findOne({{short_name:'DMI'}}, {{name:1, _id:0}})")
mongo_cmd(f"db.getSiblingDB('university_cdc').flat_data.findOne({{id:'{grp_id}'}}, {{name:1, _id:0}})")

print('\n  === MongoDB hierarchy ===')
mongo_cmd(f"var d=db.getSiblingDB('university').hierarchy.findOne(); print(d.institutes['{inst_id}'] !== undefined ? 'name: '+d.institutes['{inst_id}'].name : 'NOT_FOUND')")

# ============================================================
sep('ШАГ 3: DELETE - удаляем')
# ============================================================

print('  === PostgreSQL (источник) ===')
psql(f"DELETE FROM institute WHERE id='{inst_id}'")
psql(f"DELETE FROM student_group WHERE id='{grp_id}'")
print('\n  >>> СМОТРИ Control Center - 2 сообщения op=d (tombstone)! <<<')
pause(15)

print('  === ElasticSearch ===')
es_get('university.public.institute', 'short_name:DMI')
es_get('university.public.student_group', 'name:DEMO_UPDATED_GRP')

print('\n  === Redis ===')
redis_cmd('EXISTS', f'student_group:{grp_id}')

print('\n  === Neo4j ===')
neo4j(f"MATCH (i:Institute {{id: '{inst_id}'}}) RETURN count(i)")
neo4j(f"MATCH (sg:StudentGroup {{id: '{grp_id}'}}) RETURN count(sg)")

print('\n  === MongoDB flat ===')
mongo_cmd(f"db.getSiblingDB('university_cdc').flat_data.findOne({{short_name:'DMI'}})")
mongo_cmd(f"db.getSiblingDB('university_cdc').flat_data.findOne({{id:'{grp_id}'}})")

print('\n  === MongoDB hierarchy ===')
mongo_cmd(f"var d=db.getSiblingDB('university').hierarchy.findOne(); print(d.institutes['{inst_id}'] !== undefined ? 'FOUND' : 'DELETED')")

print('\n' + '#' * 70)
print('  ДЕМО ЗАВЕРШЕНО')
print('  Kafka UI: INSERT(op=c) -> UPDATE(op=u) -> DELETE(op=d)')
print('  Все 5 БД синхронизированы через CDC')
print('#' * 70)
