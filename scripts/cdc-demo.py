import time, subprocess, sys

def psql(cmd):
    subprocess.run(['docker', 'exec', 'postgres', 'psql', '-U', 'postgres', '-d', 'university', '-c', cmd])

def pause(msg, sec=8):
    print(f'  >>> {msg} (ждём {sec} сек — смотри в Control Center) <<<')
    time.sleep(sec)

univ_id = subprocess.check_output(
    ['docker', 'exec', 'postgres', 'psql', '-U', 'postgres', '-d', 'university', '-t', '-A', '-c',
     "SELECT id FROM university LIMIT 1"]
).decode().strip()

print('=' * 60)
print('CDC ДЕМО: INSERT → UPDATE → DELETE')
print('Control Center: http://localhost:9021')
print('Topics → university.public.institute → Messages')
print('=' * 60)

print('\n[1/3] INSERT — новый институт')
psql(f"INSERT INTO institute (id, university_id, name, short_name, dean) VALUES ('bbbb1111-0000-0000-0000-000000000001', '{univ_id}', 'DEMO_INSERT', 'DMI', 'Demo Dean')")
pause('Должно появиться сообщение с op=c (create) и name=DEMO_INSERT', 10)

print('\n[2/3] UPDATE — меняем имя института')
psql("UPDATE institute SET name='DEMO_UPDATED' WHERE short_name='DMI'")
pause('Должно появиться сообщение с op=u (update) и name=DEMO_UPDATED', 10)

print('\n[3/3] DELETE — удаляем институт')
psql("DELETE FROM institute WHERE short_name='DMI'")
pause('Должно появиться сообщение с op=d (delete) — tombstone', 10)

print('\nГотово! Все 3 операции отправлены в Kafka через Debezium CDC.')
