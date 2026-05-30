import json, urllib.request, os, time

url = 'http://localhost:8083/connectors'
dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'connectors')

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
    req = urllib.request.Request(url, data=body.encode(), headers={'Content-Type': 'application/json'})
    try:
        urllib.request.urlopen(req)
        print('[%d/6] %s OK' % (i, name))
    except urllib.error.HTTPError as e:
        print('[%d/6] %s ERROR: %s' % (i, name, json.loads(e.read()).get('message', '')[:120]))

time.sleep(30)

for name, _ in connectors:
    try:
        st = json.loads(urllib.request.urlopen(url + '/' + name + '/status').read())
        print('%s: %s' % (name, st['connector']['state']))
    except Exception:
        print('%s: FAILED' % name)
