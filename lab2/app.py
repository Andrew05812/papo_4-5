"""
Lab2 Service — необходимый объём аудитории для курса по семестру и году

Задание ЛР2:
  Выполнить запрос для извлечения отчёта о необходимом объёме аудитории для
  проведения занятий по курсу заданного семестра и года обучения с требованиями
  к использованию технических средств.
  Результат: полная информация о курсе, лекции и количестве слушателей.

Путь запроса: ТОЛЬКО Neo4j (один комплексный Cypher-запрос)

Шаг 1 — Neo4j (ЕДИНСТВЕННЫЙ шаг):
  Один Cypher-запрос, который заменяет 4 JOIN + Redis + MongoDB из реляционного подхода:
  
  1) ФИЛЬТРАЦИЯ: лекции где semester = заданный, lecture_type = 'лекция',
     computer_type содержит заданное оборудование (или точное совпадение в tags)
  
  2) ОБХОД ГРАФА (вместо 4 JOIN в SQL):
     Lecture-[BELONGS_TO]->LectureCourse         — к какому курсу относится лекция
     Lecture<-[PART_OF]-Schedule                  — когда эта лекцияscheduled
     Schedule<-[CONTAINS]-StudentGroup            — какая группа идёт на занятие
     Student-[MEMBER_OF]->StudentGroup            — какие студенты в группе
  
  3) АГРЕГАЦИЯ (вместо Redis pipeline):
     collect(st) — собираем всех студентов группы в список
     size(all_students) — количество студентов (= слушателей)
     all_students[0..10] — первые 10 студентов для отображения
  
  4) ИЕРАРХИЯ (вместо MongoDB findOne):
     LectureCourse-[FOR_SPECIALITY]->Speciality   — специальность курса
     Speciality-[PART_OF]->Department             — кафедра
     Department-[PART_OF]->Institute              — институт
     Institute-[PART_OF]->University              — университет

  Преимущество Neo4j: обход от стартовых нод за O(E) вместо O(N*M) при 4 JOIN.
  Один запрос вместо 4 отдельных запросов к PG + Redis + MongoDB.
"""
# FastAPI — веб-фреймворк для REST API
from fastapi import FastAPI, Query, Depends, HTTPException
# HTTPBearer — схема Bearer-аутентификации; HTTPAuthorizationCredentials — объект с токеном
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# Neo4j: графовая СУБД — все данные для ЛР2 берутся ТОЛЬКО отсюда
from neo4j import GraphDatabase
# jwt: PyJWT для проверки сервисных JWT-токенов (выдаются api-gateway)
import jwt
# os: переменные окружения (NEO4J_URI, JWT_SECRET и т.д.)
import os
# logging: протоколирование запросов
import logging
# time: замер времени выполнения запроса
import time
from datetime import date as date_type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Lab 2 - Schedule Capacity (Neo4j)")

# JWT-аутентификация: api-gateway выдаёт токены с type=service
JWT_SECRET = os.environ.get("JWT_SECRET", "polyglot_jwt_secret_key_2026")
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
security = HTTPBearer()


def verify_service_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Проверка JWT: токен должен быть type=service, подпись HS256, не просрочен."""
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("type") != "service":
            raise HTTPException(status_code=403, detail="Service token required")
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


# Neo4j — ЕДИНСТВЕННОЕ хранилище для ЛР2
# Данные попадают через CDC: PG → Debezium → Kafka → Neo4j Sink Connector
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASSWORD", "password12345")


def get_neo4j():
    """Создаёт драйвер Neo4j для Cypher-запроса."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


def format_date(val):
    """
    Форматирует значение даты в строку YYYY-MM-DD.
    
    Debezium pgoutput кодирует DATE как целое число дней от 1970-01-01.
    Пример: 20372 → 2025-09-22.
    """
    if val is None or val == "":
        return ""
    if isinstance(val, int):
        d = date_type(1970, 1, 1) + __import__("datetime").timedelta(days=val)
        return d.isoformat()
    if isinstance(val, str):
        return val[:10]
    return str(val)


def format_time(val):
    """
    Форматирует значение времени в строку HH:MM.
    
    Debezium может сериализовать TIME из PostgreSQL по-разному в зависимости
    от time.precision.mode:
    - 'connect' → целое число (мс с полуночи), например 28800000 (= 08:00)
    - 'adaptive' → целое число (мкс с полуночи), например 28800000000
    - иногда → строка ISO "08:00:00"
    
    Эта функция обрабатывает все варианты:
    - int → конвертирует мс в HH:MM
    - str "08:00:00" → обрезает до HH:MM
    - None / "" → "--"
    """
    if val is None or val == "":
        return "--"
    if isinstance(val, int):
        # Debezium pgoutput кодирует TIME как микросекунды с полуночи
        # Пример: 39600000000 мкс = 39600 сек = 11:00
        if val > 1_000_000_000:
            total_seconds = val // 1_000_000  # мкс → сек
        else:
            total_seconds = val // 1000  # мс → сек
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours:02d}:{minutes:02d}"
    if isinstance(val, str):
        # "08:00:00" → "08:00"
        return val[:5] if len(val) >= 5 else val
    return str(val)


@app.get("/query")
def query_schedule_capacity(
    semester: int = Query(..., description="Номер семестра (1-8)"),
    year: int = Query(..., description="Год обучения"),
    equipment: str = Query("", description="Требования к компьютерному обеспечению"),
    _=Depends(verify_service_token)
):
    """
    ЛР2: Определение необходимого объёма аудитории для курса по семестру и году.
    
    Путь: ТОЛЬКО Neo4j (один комплексный Cypher-запрос).
    
    Параметры:
      semester — номер семестра (1-8), фильтр по LectureCourse.semester
      year — год обучения, фильтр по Schedule.date (внутри этого года)
      equipment — требование к оборудованию, поиск в Lecture.computer_type или Lecture.tags
                  пустая строка = не фильтровать по оборудованию
    """
    steps = []
    start = time.time()

    # ===== ЕДИНСТВЕННЫЙ ШАГ: Neo4j =====
    # Один Cypher-запрос заменяет 4 операции реляционного подхода:
    #   1) Фильтрация лекций по semester + computer_type + lecture_type
    #   2) Обход графа: Lecture→Course, Schedule←Group←Student (вместо 4 JOIN)
    #   3) collect()/size() для COUNT студентов (вместо Redis HGETALL)
    #   4) Иерархия Course→Speciality→Department→Institute→University (вместо MongoDB)
    logger.info(f"Step 1: Neo4j - semester={semester}, year={year}, equipment='{equipment}'")
    driver = get_neo4j()

    # Границы года для фильтрации по дате расписания
    # Debezium хранит DATE как integer (дни от эпохи 1970-01-01) при adaptive time.precision.mode
    # Поэтому сравниваем sch.date с epoch-днями, а не со строками 'YYYY-MM-DD'
    epoch = date_type(1970, 1, 1)
    year_start = (date_type(year, 1, 1) - epoch).days
    year_end = (date_type(year, 12, 31) - epoch).days

    # Структуры для сбора данных из Cypher-результата
    course_info = {}          # course_id → {name, semester, hours...}
    lecture_info = {}         # lecture_id → {title, type, computer_type, tags, course_id}
    schedule_by_lecture = {}  # lecture_id → [{schedule_id, group_id, classroom, date, time, teacher}]
    group_data = {}           # group_id → {name, student_count, students_sample}
    hierarchy_info = {}       # speciality_id → {university, institute, department, speciality, code}

    with driver.session() as session:
        # ==============================================================
        # ОСНОВНОЙ CYPHER-ЗАПРОС
        #
        # Что делает (по шагам внутри запроса):
        #
        # 1. MATCH (l:Lecture) WHERE l.type = 'лекция'
        #    — Находим все лекции типа "лекция" (не "практика" и не "лабораторная")
        #    — l.type установлен Neo4j Sink из PG: l.type = event.after.lecture_type
        #
        # 2. AND ($equipment = '' OR toLower(l.computer_type) CONTAINS toLower($equipment) OR $equipment IN l.tags)
        #    — Фильтр по оборудованию: если equipment пустой — не фильтруем
        #    — Иначе ищем equipment в computer_type (регистронезависимый CONTAINS)
        #      или точное совпадение в массиве tags
        #    — l.computer_type установлен Sink из PG: event.after.computer_type
        #      (может быть null если лекция не требует оборудования — 60% лекций)
        #    — l.tags — массив строк из PG TEXT[] (например: ["спецдисциплина", "базы_данных"])
        #
        # 3. MATCH (l)-[:BELONGS_TO]->(c:LectureCourse) WHERE c.semester = $semester
        #    — Переход от лекции к курсу через связь BELONGS_TO
        #    — Фильтруем по семестру (semester — номер семестра курса)
        #
        # 4. MATCH (l)<-[:PART_OF]-(sch:Schedule)
        #    WHERE sch.date >= $year_start AND sch.date <= $year_end
        #    — Переход от лекции к расписанию через связь PART_OF (Schedule→Lecture)
        #    — Фильтруем по дате: только расписания внутри указанного года
        #    — sch.classroom, sch.start_time, sch.end_time, sch.teacher_name
        #      установлены Sink из PG schedule
        #
        # 5. MATCH (sch)<-[:CONTAINS]-(g:StudentGroup)
        #    — Переход от расписания к группе через связь CONTAINS (Group→Schedule)
        #    — Одна группа может иметь много расписаний (разные занятия)
        #
        # 6. OPTIONAL MATCH (c)-[:FOR_SPECIALITY]->(sp:Speciality)-[:PART_OF]->(d:Department)-[:PART_OF]->(i:Institute)-[:PART_OF]->(u:University)
        #    — Иерархия: Course→Speciality→Department→Institute→University
        #    — OPTIONAL: если связи нет (нет специальности) — не ломает запрос
        #    — Заменяет MongoDB findOne для получения иерархии
        #
        # 7. OPTIONAL MATCH (st:Student)-[:MEMBER_OF]->(g)
        #    — Находим всех студентов в группе через связь MEMBER_OF
        #    — OPTIONAL: если в группе нет студентов — не ломает запрос
        #
        # 8. WITH l, c, sch, g, sp, d, i, u, collect(st) AS all_students
        #    — Собираем студентов группы в список (aggregation)
        #    — collect() — аналог GROUP BY + aggregate в SQL
        #
        # 9. RETURN l.id, l.title, l.type, l.computer_type, l.tags,
        #          c.id, c.name, c.semester, c.total_hours, c.lecture_hours, c.practice_hours, c.lab_hours,
        #          sch.id, sch.date, sch.start_time, sch.end_time, sch.classroom, sch.teacher_name,
        #          g.id, g.name,
        #          size(all_students) AS student_count,                    — количество студентов (= слушателей)
        #          [s IN all_students[0..10] | {...}] AS students_sample,  — первые 10 студентов
        #          sp.id, sp.name, sp.code, d.name, i.name, u.name         — иерархия
        # ==============================================================
        result = session.run("""
            MATCH (l:Lecture)
            WHERE l.type = 'лекция'
              AND ($equipment = '' OR toLower(l.computer_type) CONTAINS toLower($equipment) OR $equipment IN l.tags)
            MATCH (l)-[:BELONGS_TO]->(c:LectureCourse)
            WHERE c.semester = $semester
            MATCH (l)<-[:PART_OF]-(sch:Schedule)
            WHERE sch.date >= $year_start AND sch.date <= $year_end
            MATCH (sch)<-[:CONTAINS]-(g:StudentGroup)
            OPTIONAL MATCH (c)-[:FOR_SPECIALITY]->(sp:Speciality)-[:PART_OF]->(d:Department)-[:PART_OF]->(i:Institute)-[:PART_OF]->(u:University)
            OPTIONAL MATCH (st:Student)-[:MEMBER_OF]->(g)
            WITH l, c, sch, g, sp, d, i, u,
                 collect(st) AS all_students
            RETURN l.id AS lecture_id, l.title AS lecture_title, l.type AS lecture_type,
                   l.computer_type AS computer_type, l.tags AS tags,
                   c.id AS course_id, c.name AS course_name, c.semester AS course_semester,
                   c.total_hours AS course_total_hours,
                   c.lecture_hours AS course_lecture_hours, c.practice_hours AS course_practice_hours,
                   c.lab_hours AS course_lab_hours, c.description AS course_description,
                   sch.id AS schedule_id, sch.date AS scheduled_date,
                   sch.start_time AS start_time, sch.end_time AS end_time, sch.classroom AS classroom, sch.teacher_name AS teacher_name,
                   g.id AS group_id, g.name AS group_name,
                   size(all_students) AS student_count,
                   [s IN all_students[0..10] | {id: s.id, first_name: s.first_name, last_name: s.last_name, patronymic: s.patronymic, student_card_number: s.card_number}] AS students_sample,
                   sp.id AS speciality_id, sp.name AS speciality_name, sp.code AS speciality_code,
                   d.name AS department_name, d.short_name AS department_short,
                   i.name AS institute_name, i.short_name AS institute_short,
                   u.name AS university_name
        """, semester=semester, year_start=year_start, year_end=year_end, equipment=equipment)

        # Обработка результатов Cypher-запроса
        # Каждый record = одна строка результата (лекция × расписание × группа)
        # Если лекция имеет 2 расписания для 2 групп → 4 записи
        for record in result:
            # --- Собираем информацию о курсе (уникальную по course_id) ---
            cid = record.get("course_id")
            if cid and cid not in course_info:
                course_info[cid] = {
                    "name": record.get("course_name") or "",
                    "semester": record.get("course_semester") or 0,
                    "total_hours": record.get("course_total_hours") or 0,
                    "lecture_hours": record.get("course_lecture_hours") or 0,
                    "practice_hours": record.get("course_practice_hours") or 0,
                    "lab_hours": record.get("course_lab_hours") or 0,
                    "description": record.get("course_description") or ""
                }

            # --- Собираем информацию о лекции (уникальную по lecture_id) ---
            # computer_type может быть null (60% лекций не требуют оборудования)
            # tags — массив строк (TEXT[] из PG)
            lid = record.get("lecture_id")
            if lid and lid not in lecture_info:
                lecture_info[lid] = {
                    "title": record.get("lecture_title") or "",
                    "type": record.get("lecture_type") or "",
                    "computer_type": record.get("computer_type") or "",
                    "tags": record.get("tags") or [],
                    "course_id": cid
                }

            # --- Собираем расписание для каждой лекции ---
            # classroom, start_time, end_time, teacher_name — из Schedule-узла
            # Могут быть null если CDC sink не успел их установить
            if lid:
                if lid not in schedule_by_lecture:
                    schedule_by_lecture[lid] = []
                schedule_by_lecture[lid].append({
                    "schedule_id": record.get("schedule_id") or "",
                    "group_id": record.get("group_id") or "",
                    "classroom": record.get("classroom") or "",
                    "date": record.get("scheduled_date") or "",
                    "start_time": record.get("start_time"),   # Может быть int или str — форматируем ниже
                    "end_time": record.get("end_time"),       # Может быть int или str — форматируем ниже
                    "teacher": record.get("teacher_name") or ""
                })

            # --- Собираем данные о группах (уникальные по group_id) ---
            # student_count = size(all_students) — количество студентов в группе
            # students_sample — первые 10 студентов (вместо Redis HGETALL)
            gid = record.get("group_id")
            if gid and gid not in group_data:
                group_data[gid] = {
                    "name": record.get("group_name") or "",
                    "student_count": record.get("student_count") or 0,
                    "students_sample": record.get("students_sample") or []
                }

            # --- Собираем иерархию (уникальную по speciality_id) ---
            # University→Institute→Department→Speciality (из графа Neo4j)
            spid = record.get("speciality_id")
            if spid and spid not in hierarchy_info:
                hierarchy_info[spid] = {
                    "university": record.get("university_name") or "",
                    "institute": record.get("institute_name") or "",
                    "department": record.get("department_name") or "",
                    "speciality": record.get("speciality_name") or "",
                    "speciality_code": record.get("speciality_code") or ""
                }

    driver.close()

    # Подсчитываем listeners для каждой лекции = сумма student_count по всем группам
    lecture_group_ids = {}
    for lid, scheds in schedule_by_lecture.items():
        gids = set(sch["group_id"] for sch in scheds)
        lecture_group_ids[lid] = gids

    total_lectures = len(lecture_info)
    total_schedules = sum(len(v) for v in schedule_by_lecture.values())

    steps.append({
        "step": 1,
        "store": "Neo4j",
        "action": f"Комплексный Cypher: фильтр (semester={semester}, computer_type='{equipment}', type='лекция') + обход Lecture→Course→Schedule→Group→Student + иерархия Course→Spec→Dept→Inst→Univ + collect()/size()",
        "result": f"Найдено {total_lectures} лекций, {total_schedules} расписаний, {len(group_data)} групп"
    })

    if not lecture_info:
        return {"result": [], "steps": steps, "query_path": "Neo4j",
                "execution_time_sec": round(time.time() - start, 3)}

    # ===== ФИНАЛЬНАЯ СБОРКА =====
    # Группируем лекции по курсам (один курс = несколько лекций)
    course_lectures = {}
    for lid, li in lecture_info.items():
        cid = li["course_id"]
        course_lectures.setdefault(cid, set()).add(lid)

    # Собираем группы, привязанные к курсу через расписание
    course_groups = {}
    for lid, gids in lecture_group_ids.items():
        cid = lecture_info[lid]["course_id"]
        course_groups.setdefault(cid, set()).update(gids)

    final_results = []
    for cid, lec_ids in course_lectures.items():
        ci = course_info.get(cid, {})

        # Иерархия — берём первую найденную (все лекции курса имеют одну иерархию)
        hi = {}
        for spid, h in hierarchy_info.items():
            hi = h
            break

        # --- Детализация лекций: classroom, date, time, teacher, listeners ---
        lecture_details = []
        for lid in lec_ids:
            li = lecture_info.get(lid, {})
            scheds = schedule_by_lecture.get(lid, [])
            gids_for_lecture = lecture_group_ids.get(lid, set())
            # listeners = сумма студентов во всех группах, идущих на эту лекцию
            listeners = sum(
                group_data.get(gid, {}).get("student_count", 0)
                for gid in gids_for_lecture
            )
            for sch in scheds:
                lecture_details.append({
                    "lecture_id": lid,
                    "title": li.get("title", ""),
                    "type": li.get("type", ""),
                    # computer_type: может быть пустой — 60% лекций не требуют оборудования
                    "computer_type": li.get("computer_type", ""),
                    "tags": li.get("tags", []),
                    "classroom": sch["classroom"],
                    # Дата: Debezium pgoutput кодирует DATE как целое число дней от 1970-01-01
                    "date": format_date(sch["date"]),
                    # Время: форматируем через format_time() — обрабатывает мкс и ISO-строки
                    "time": f"{format_time(sch['start_time'])}-{format_time(sch['end_time'])}",
                    "teacher": sch["teacher"],
                    "listeners": listeners
                })

        # max_listeners — максимальное кол-во слушателей на одной лекции
        # = необходимая вместимость аудитории (main metric ЛР2)
        max_listeners = max((l["listeners"] for l in lecture_details), default=0)

        # --- Информация о группах: студенты из Neo4j (первые 10) ---
        groups_info = []
        for gid in course_groups.get(cid, set()):
            gd = group_data.get(gid, {})
            groups_info.append({
                "id": gid,
                "name": gd.get("name", ""),
                "student_count": gd.get("student_count", 0),
                "students": gd.get("students_sample", [])
            })

        # Иерархия из Neo4j: University→Institute→Department→Speciality
        spec_id = ""
        for lid in lec_ids:
            for spid, h in hierarchy_info.items():
                hi = h
                spec_id = spid
                break
            if spec_id:
                break

        final_results.append({
            "course": {
                "id": cid,
                "name": ci.get("name", ""),
                "semester": ci.get("semester", 0),
                "total_hours": ci.get("total_hours", 0),
                "lecture_hours": ci.get("lecture_hours", 0),
                "practice_hours": ci.get("practice_hours", 0),
                "lab_hours": ci.get("lab_hours", 0),
                "description": ci.get("description", "")
            },
            "groups": groups_info,
            "lectures": lecture_details,
            "total_listeners": sum(
                group_data.get(gid, {}).get("student_count", 0)
                for gid in course_groups.get(cid, set())
            ),
            # max_listeners_per_lecture = минимальная вместимость аудитории для курса
            "max_listeners_per_lecture": max_listeners,
            "required_classroom_capacity": max_listeners,
            "hierarchy": hi
        })

    elapsed = round(time.time() - start, 3)

    return {
        "result": final_results,
        "steps": steps,
        "query_path": "Neo4j",
        "execution_time_sec": elapsed,
        "justification": {
            "Neo4j": "Один Cypher-запрос: фильтрация + обход графа (Lecture→Course→Schedule→Group→Student) + иерархия (Course→Speciality→Department→Institute→University) + collect()/size() вместо 4 JOIN в PG + Redis pipeline + MongoDB findOne"
        }
    }


@app.get("/")
def root():
    return {"service": "lab2", "description": "Schedule capacity: Neo4j"}
