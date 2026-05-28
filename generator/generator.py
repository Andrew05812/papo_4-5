"""
Модуль generator.py — основная логика генератора тестовых данных.

Заполняет ТОЛЬКО PostgreSQL (12 таблиц, включая партиционированную таблицу attendance).
Остальные СУБД (Elasticsearch, Neo4j, Redis, MongoDB) заполняются через CDC pipeline:
  PostgreSQL → Debezium Source → Kafka → Sink Connectors → 4 БД

ES индекс 'lectures' (BM25, russian_custom анализатор) создаётся Lab1 при старте,
а НЕ генератором — для соответствия требованию "генератор заполняет только PostgreSQL".
"""
import psycopg2
from psycopg2.extras import execute_values
import uuid
import random
# date, time, datetime, timedelta: работа с датами расписания и посещаемости
from datetime import date, time, datetime, timedelta
# Optional: аннотации типов для функций (в текущей версии не используется активно)
from typing import Optional
# logging: протоколирование процесса генерации (INFO-уровень)
import logging
# os: чтение переменных окружения для подключения к PostgreSQL
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Мужские имена для генерации тестовых данных студентов
FIRST_NAMES_M = [
    "Александр", "Дмитрий", "Максим", "Сергей", "Андрей", "Алексей", "Артём",
    "Илья", "Кирилл", "Михаил", "Никита", "Матвей", "Роман", "Егор", "Арсений",
    "Иван", "Денис", "Евгений", "Даниил", "Тимофей", "Владислав", "Игорь",
    "Владимир", "Павел", "Руслан", "Марк", "Константин", "Тимур", "Олег", "Ярослав"
]
# Женские имена
FIRST_NAMES_F = [
    "Анастасия", "Мария", "Дарья", "Елена", "Анна", "Виктория", "Екатерина",
    "Алина", "Ирина", "Полина", "Ольга", "Татьяна", "Ксения", "Валерия", "Софья",
    "Юлия", "Марина", "Людмила", "Наталья", "Светлана", "Елизавета", "Вероника",
    "Александра", "Мирослава", "Варвара", "Диана", "Кристина", "Надежда", "Оксана", "Евгения"
]
# Мужские фамилии
LAST_NAMES_M = [
    "Иванов", "Смирнов", "Кузнецов", "Попов", "Васильев", "Петров", "Соколов",
    "Михайлов", "Новиков", "Фёдоров", "Морозов", "Волков", "Алексеев", "Лебедев",
    "Семёнов", "Егоров", "Павлов", "Козлов", "Степанов", "Николаев", "Орлов",
    "Андреев", "Макаров", "Никитов", "Захаров", "Зайцев", "Соловьёв", "Борисов",
    "Яковлев", "Григорьев"
]
# Женские фамилии
LAST_NAMES_F = [
    "Иванова", "Смирнова", "Кузнецова", "Попова", "Васильева", "Петрова", "Соколова",
    "Михайлова", "Новикова", "Фёдорова", "Морозова", "Волкова", "Алексеева", "Лебедева",
    "Семёнова", "Егорова", "Павлова", "Козлова", "Степанова", "Николаева", "Орлова",
    "Андреева", "Макарова", "Никитина", "Захарова", "Зайцева", "Соловьёва", "Борисова",
    "Яковлева", "Григорьева"
]
# Мужские отчества
PATRONYMICS_M = [
    "Александрович", "Дмитриевич", "Сергеевич", "Андреевич", "Алексеевич",
    "Михайлович", "Владимирович", "Игоревич", "Николаевич", "Евгеньевич",
    "Павлович", "Иванович", "Олегович", "Романович", "Кириллович"
]
# Женские отчества
PATRONYMICS_F = [
    "Александровна", "Дмитриевна", "Сергеевна", "Андреевна", "Алексеевна",
    "Михайловна", "Владимировна", "Игоревна", "Николаевна", "Евгеньевна",
    "Павловна", "Ивановна", "Олеговна", "Романовна", "Кирилловна"
]

# 5 институтов университета (справочник)
INSTITUTES = [
    {"name": "Институт информационных технологий", "short": "ИИТ", "dean": "Петров П.П."},
    {"name": "Институт радиотехнических систем", "short": "ИРТС", "dean": "Сидоров С.С."},
    {"name": "Институт компьютерных технологий", "short": "ИКТ", "dean": "Козлов К.К."},
    {"name": "Институт кибербезопасности", "short": "ИКБ", "dean": "Волков В.В."},
    {"name": "Институт перспективных технологий и индустрии", "short": "ИПТИ", "dean": "Морозов М.М."},
]

# 15 кафедр (inst — индекс института в массиве INSTITUTES)
DEPARTMENTS = [
    {"name": "Кафедра программной инженерии", "short": "КПИ", "head": "Белов Б.Б.", "inst": 0},
    {"name": "Кафедра информационных систем", "short": "КИС", "head": "Громов Г.Г.", "inst": 0},
    {"name": "Кафедра вычислительной техники", "short": "КВТ", "head": "Орлов О.О.", "inst": 0},
    {"name": "Кафедра радиотехники", "short": "КРТ", "head": "Лебедев Л.Л.", "inst": 1},
    {"name": "Кафедра телекоммуникаций", "short": "КТел", "head": "Соколов С.С.", "inst": 1},
    {"name": "Кафедра сигнальных систем", "short": "КСС", "head": "Зайцев З.З.", "inst": 1},
    {"name": "Кафедра искусственного интеллекта", "short": "КИИ", "head": "Мишин М.М.", "inst": 2},
    {"name": "Кафедра системного анализа", "short": "КСА", "head": "Воронов В.В.", "inst": 2},
    {"name": "Кафедра машинного обучения", "short": "КМО", "head": "Фролов Ф.Ф.", "inst": 2},
    {"name": "Кафедра информационной безопасности", "short": "КИБ", "head": "Крутов К.К.", "inst": 3},
    {"name": "Кафедра криптографии", "short": "ККрипт", "head": "Жуков Ж.Ж.", "inst": 3},
    {"name": "Кафедра сетевой безопасности", "short": "КСБ", "head": "Громов Г.Г.", "inst": 3},
    {"name": "Кафедра нанотехнологий", "short": "КНТ", "head": "Титов Т.Т.", "inst": 4},
    {"name": "Кафедра биотехнологий", "short": "КБТ", "head": "Лисов Л.Л.", "inst": 4},
    {"name": "Кафедра оптоэлектроники", "short": "КОЭ", "head": "Медведев М.М.", "inst": 4},
]

# 30 специальностей с кодами, уровнем и длительностью обучения
SPECIALITIES = [
    {"name": "Программная инженерия", "code": "09.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Информационные системы и технологии", "code": "09.03.02", "degree": "бакалавр", "duration": 4},
    {"name": "Вычислительные машины, комплексы и системы", "code": "09.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Радиотехника", "code": "11.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Инфокоммуникационные технологии и системы связи", "code": "11.03.02", "degree": "бакалавр", "duration": 4},
    {"name": "Конструирование и технология электронных средств", "code": "11.03.03", "degree": "бакалавр", "duration": 4},
    {"name": "Интеллектуальные системы в гуманитарной сфере", "code": "09.03.03", "degree": "бакалавр", "duration": 4},
    {"name": "Прикладная информатика", "code": "09.03.03", "degree": "бакалавр", "duration": 4},
    {"name": "Математическое обеспечение и администрирование информационных систем", "code": "09.03.03", "degree": "бакалавр", "duration": 4},
    {"name": "Информационная безопасность автоматизированных систем", "code": "10.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Информационная безопасность", "code": "10.05.01", "degree": "специалист", "duration": 5},
    {"name": "Криптография и кибербезопасность", "code": "10.05.02", "degree": "специалист", "duration": 5},
    {"name": "Нанотехнологии и микросистемная техника", "code": "28.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Биотехнология", "code": "19.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Оптотехника", "code": "12.03.02", "degree": "бакалавр", "duration": 4},
    {"name": "Компьютерные науки", "code": "09.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Сетевая безопасность", "code": "10.03.02", "degree": "бакалавр", "duration": 4},
    {"name": "Системный анализ и управление", "code": "27.03.03", "degree": "бакалавр", "duration": 4},
    {"name": "Машинное обучение и data science", "code": "09.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Телекоммуникационные системы", "code": "11.03.02", "degree": "бакалавр", "duration": 4},
    {"name": "Программирование и алгоритмика", "code": "09.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Кибернетика", "code": "09.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Электроника и наноэлектроника", "code": "11.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Биоинформатика", "code": "19.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Инженерия программного обеспечения", "code": "09.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Анализ данных", "code": "09.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Защита информации", "code": "10.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Робототехника", "code": "09.03.04", "degree": "бакалавр", "duration": 4},
    {"name": "Цифровая обработка сигналов", "code": "11.03.01", "degree": "бакалавр", "duration": 4},
    {"name": "Архитектура вычислительных систем", "code": "09.03.01", "degree": "бакалавр", "duration": 4},
]

# Названия 60 учебных курсов
COURSE_NAMES = [
    "Основы программирования", "Алгоритмы и структуры данных", "Базы данных",
    "Операционные системы", "Компьютерные сети", "Дискретная математика",
    "Математический анализ", "Линейная алгебра", "Теория вероятностей",
    "Объектно-ориентированное программирование", "Функциональное программирование",
    "Веб-технологии", "Мобильная разработка", "Проектирование информационных систем",
    "Интеллектуальные системы", "Машинное обучение", "Нейронные сети",
    "Компьютерная графика", "Обработка изображений", "Робототехника",
    "Информационная безопасность", "Криптография", "Сетевая безопасность",
    "Системное программирование", "Параллельные вычисления", "Распределённые системы",
    "Архитектура ЭВМ", "Микропроцессорные системы", "Цифровая обработка сигналов",
    "Радиотехнические цепи и сигналы", "Теория электрических цепей",
    "Электроника", "Схемотехника", "Телекоммуникационные технологии",
    "Оптоволоконные системы связи", "Антенные системы", "Распространение радиоволн",
    "Наноматериалы", "Биоинформатика", "Оптоэлектронные приборы",
    "Квантовые вычисления", "Блокчейн-технологии", "DevOps и CI/CD",
    "Data Engineering", "Big Data", "Облачные технологии",
    "Интернет вещей", "Кибербезопасность", "Цифровая трансформация",
    "Программная инженерия", "Управление проектами", "Системный анализ",
    "Математическое моделирование", "Численные методы", "Статистика",
    "Физика", "Прикладная механика", "Экология",
    "Философия", "История", "Иностранный язык",
    "Правоведение", "Экономика", "Менеджмент",
]

# Типы компьютерного обеспечения
COMPUTER_TYPES = [
    "проектор", "компьютерный класс", "интерактивная доска", "лабораторное оборудование",
    "микроконтроллеры", "осциллограф", "мультимедийный проектор", "документ-камера",
    "система видеоконференцсвязи", "VR-оборудование", "серверное оборудование",
    "сетевое оборудование", "3D-принтер", "робототехнический комплект",
    "плата Arduino", "плата Raspberry Pi", "GPU-ускоритель", "кластер HPC",
]

# Типы занятий: лекция, практика, лабораторная
LECTURE_TYPES = ["лекция", "практика", "лабораторная"]

# Описания курсов (для лекций с подробным содержанием)
COURSE_DESCRIPTIONS = {
    "Основы программирования": "Изучение базовых принципов программирования на языках высокого уровня. Переменные, типы данных, операторы, циклы, функции. Введение в алгоритмику.",
    "Алгоритмы и структуры данных": "Классические алгоритмы сортировки, поиска, графовые алгоритмы. Структуры данных: списки, деревья, хеш-таблицы, графы.",
    "Базы данных": "Реляционные модели данных, SQL, проектирование баз данных, нормализация, индексирование, транзакции.",
    "Операционные системы": "Архитектура ОС, управление процессами, памятью, файловыми системами. Linux, Windows Server.",
    "Компьютерные сети": "Модель OSI, TCP/IP, маршрутизация, коммутация, протоколы прикладного уровня, сетевая безопасность.",
    "Машинное обучение": "Методы обучения с учителем и без учителя. Регрессия, классификация, кластеризация, нейронные сети.",
    "Нейронные сети": "Архитектуры нейронных сетей: CNN, RNN, Transformer. Обучение, регуляризация, оптимизация.",
    "Информационная безопасность": "Угрозы информационной безопасности, методы защиты, криптографические протоколы, аутентификация.",
    "Микропроцессорные системы": "Архитектура микропроцессоров, система команд, прерывания, периферийные устройства, встраиваемые системы.",
    "Криптография": "Симметричное и асимметричное шифрование, хеш-функции, цифровые подписи, протоколы TLS/SSL.",
}

# Шаблоны аннотаций лекций
LECTURE_ANNOTATIONS = [
    "Введение в предметную область. Основные понятия и определения.",
    "Обзор литературы и источников. Методология исследования.",
    "Классификация и систематизация материала. Базовые концепции.",
    "Практическое применение теоретических знаний. Разбор примеров.",
    "Анализ современных подходов и методов. Сравнительный обзор.",
    "Углублённое изучение ключевых аспектов. Расширенный анализ.",
    "Решение типовых задач. Практические задания и упражнения.",
    "Обзор инструментальных средств. Среды разработки и отладки.",
    "Проектирование и моделирование. Архитектурные паттерны.",
    "Тестирование и верификация. Методы контроля качества.",
    "Оптимизация и масштабирование. Производительность систем.",
    "Интеграция и развёртывание. DevOps-практики.",
]

# Шаблоны текстов материалов лекций ({topic} — подставляется тема)
LECTURE_CONTENT_TEMPLATES = [
    "Рассматриваются принципы работы {topic}. Основные характеристики и параметры. Примеры использования в реальных системах. Методы анализа и синтеза.",
    "Изучение методов {topic}. Теоретические основы и практическое применение. Алгоритмы реализации и оптимизации. Современные тенденции развития.",
    "Анализ архитектуры {topic}. Компоненты и их взаимодействие. Проектирование и отладка. Инструменты разработки и тестирования.",
    "Обзор технологий {topic}. Сравнение подходов и решений. Критерии выбора и оценки. Перспективы развития и применения.",
    "Практикум по {topic}. Пошаговое выполнение заданий. Типичные ошибки и способы их устранения. Рекомендации по улучшению.",
]

# Теги специальных дисциплин для фильтрации в ЛР3
SPECIAL_TAGS = [
    "спецдисциплина", "кафедральная_дисциплина", "профильная_дисциплина",
    "дисциплина_кафедры", "специализация"
]

# Номера аудиторий
CLASSROOMS = [
    "А-101", "А-102", "А-201", "А-202", "А-301", "А-302",
    "Б-101", "Б-102", "Б-201", "Б-202", "Б-301", "Б-302",
    "В-101", "В-102", "В-201", "В-202", "В-301", "В-302",
    "ИВЦ-101", "ИВЦ-102", "ИВЦ-201", "ИВЦ-202", "ИВЦ-301", "ИВЦ-302",
    "К-101", "К-102", "К-201", "К-202", "К-301", "К-302",
]

# ФИО преподавателей
TEACHER_NAMES = [
    "Проф. Иванов И.И.", "Доц. Петров П.П.", "Доц. Сидоров С.С.",
    "Проф. Козлов К.К.", "Доц. Волков В.В.", "Проф. Морозов М.М.",
    "Доц. Лебедев Л.Л.", "Проф. Соколов С.С.", "Доц. Зайцев З.З.",
    "Проф. Орлов О.О.", "Доц. Макаров М.М.", "Проф. Белов Б.Б.",
    "Доц. Громов Г.Г.", "Проф. Мишин М.М.", "Доц. Фролов Ф.Ф.",
    "Проф. Крутов К.К.", "Доц. Жуков Ж.Ж.", "Проф. Титов Т.Т.",
    "Доц. Лисов Л.Л.", "Проф. Медведев М.М.", "Доц. Воронов В.В.",
]


# Подключение к PostgreSQL (реляционная БД — master-источник всех сущностей)
def get_pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"]
    )


def generate_data(student_count=1000):
    """
    Главная функция генерации. Поток:
    1. Очистка PostgreSQL (clear_postgres)
    2. Заполнение PostgreSQL (12 таблиц: university → institute → department →
       speciality → department_specialities → lecture_course → lecture →
       lecture_material → student_group → student → schedule → attendance)

    Остальные СУБД (Elasticsearch, Neo4j, Redis, MongoDB) заполняются через CDC pipeline:
      PostgreSQL → Debezium Source → Kafka → Sink Connectors

    ES индекс 'lectures' для Lab1 создаётся Lab1 при старте (ensure_lectures_index),
    а НЕ генератором — для соответствия требованию "генератор заполняет только PostgreSQL".
    """
    logger.info(f"Starting data generation: {student_count} students")
    clear_postgres()

    random.seed(42)

    university_id = str(uuid.uuid4())
    institute_ids = [str(uuid.uuid4()) for _ in INSTITUTES]
    department_ids = [str(uuid.uuid4()) for _ in DEPARTMENTS]
    speciality_ids = [str(uuid.uuid4()) for _ in SPECIALITIES]
    dept_spec_ids = []
    course_ids = []
    lecture_ids = []
    lecture_material_ids = []
    group_ids = []
    student_ids = []
    schedule_ids = []

    pg = get_pg_conn()
    cur = pg.cursor()

    try:
        # --- Вставка университета (корневая запись, 1 строка) ---
        cur.execute(
            "INSERT INTO university (id, name, short_name, address, founded_year) VALUES (%s, %s, %s, %s, %s)",
            (university_id, "РТУ МИРЭА", "МИРЭА", "г. Москва, проспект Вернадского, 78", 1947)
        )

        # --- Вставка институтов (5 строк, FK → university) ---
        for i, inst in enumerate(INSTITUTES):
            cur.execute(
                "INSERT INTO institute (id, university_id, name, short_name, dean) VALUES (%s, %s, %s, %s, %s)",
                (institute_ids[i], university_id, inst["name"], inst["short"], inst["dean"])
            )

        # --- Вставка кафедр (15 строк, FK → institute) ---
        for i, dept in enumerate(DEPARTMENTS):
            cur.execute(
                "INSERT INTO department (id, institute_id, name, short_name, head) VALUES (%s, %s, %s, %s, %s)",
                (department_ids[i], institute_ids[dept["inst"]], dept["name"], dept["short"], dept["head"])
            )

        # --- Вставка специальностей (30 строк, справочник) ---
        for i, spec in enumerate(SPECIALITIES):
            cur.execute(
                "INSERT INTO speciality (id, name, code, degree_level, duration_years) VALUES (%s, %s, %s, %s, %s)",
                (speciality_ids[i], spec["name"], spec["code"], spec["degree"], spec["duration"])
            )

        # --- Связь кафедра↔специальность (M:N, ~30 строк) ---
        for i in range(len(DEPARTMENTS)):
            spec_indices = [i * 2, i * 2 + 1] if i * 2 + 1 < len(SPECIALITIES) else [i * 2]
            for si in spec_indices:
                if si < len(SPECIALITIES):
                    ds_id = str(uuid.uuid4())
                    dept_spec_ids.append(ds_id)
                    cur.execute(
                        "INSERT INTO department_specialities (id, department_id, speciality_id, is_primary) VALUES (%s, %s, %s, %s)",
                        (ds_id, department_ids[i], speciality_ids[si], random.choice([True, False]))
                    )

        # Привязка курсов к специальностям (3-5 курсов на специальность)
        spec_courses = {sid: [] for sid in speciality_ids}
        course_data = []
        course_idx = 0
        for si, spec_id in enumerate(speciality_ids):
            courses_per_spec = 3 + (si % 3)
            for j in range(courses_per_spec):
                cid = str(uuid.uuid4())
                course_ids.append(cid)
                spec_courses[spec_id].append(cid)
                semester = (j % 4) + 1
                course_name = COURSE_NAMES[course_idx % len(COURSE_NAMES)]
                total_hours = random.choice([72, 108, 144, 180])
                lecture_hours = total_hours // 3
                practice_hours = total_hours // 3
                lab_hours = total_hours - lecture_hours - practice_hours
                desc = COURSE_DESCRIPTIONS.get(course_name, f"Изучение дисциплины {course_name}. Теоретические основы и практические навыки.")
                course_data.append((cid, spec_id, course_name, desc, semester, total_hours, lecture_hours, practice_hours, lab_hours))
                course_idx += 1

        # --- Вставка учебных курсов lecture_course (batch execute_values, ~100 строк) ---
        execute_values(cur,
            "INSERT INTO lecture_course (id, speciality_id, name, description, semester, total_hours, lecture_hours, practice_hours, lab_hours) VALUES %s",
            course_data
        )

        course_to_idx = {cid: ci for ci, cid in enumerate(course_ids)}
        num_lectures_per_course = 10
        lecture_data = []
        # Генерация лекций (10 лекций на курс, 30% получают спец. тег, 40% — требование оборудования)
        for ci, cid in enumerate(course_ids):
            course_name = COURSE_NAMES[ci % len(COURSE_NAMES)]
            for li in range(num_lectures_per_course):
                lid = str(uuid.uuid4())
                lecture_ids.append(lid)
                ltype = random.choice(LECTURE_TYPES)
                title = f"{course_name}: {LECTURE_ANNOTATIONS[li % len(LECTURE_ANNOTATIONS)]}"
                annotation = LECTURE_ANNOTATIONS[li % len(LECTURE_ANNOTATIONS)]
                order_number = li + 1
                dur = 90 if ltype == "лекция" else 90
                tags = []
                if random.random() < 0.3:
                    tags.append(random.choice(SPECIAL_TAGS))
                if random.random() < 0.5:
                    tags.append(course_name.lower().replace(" ", "_"))
                eq = random.choice(COMPUTER_TYPES) if random.random() < 0.4 else None
                lecture_data.append((lid, cid, title, annotation, ltype, order_number, dur, eq, tags))

        # --- Вставка лекций lecture (batch по 500, ~1000 строк, FK → lecture_course) ---
        batch_size = 500
        for i in range(0, len(lecture_data), batch_size):
            execute_values(cur,
                'INSERT INTO lecture (id, course_id, title, annotation, lecture_type, order_number, duration_minutes, computer_type, tags) VALUES %s',
                lecture_data[i:i+batch_size]
            )

        # --- Вставка материалов к лекциям lecture_material (1-3 материала на лекцию) ---
        # Генерация учебных материалов к лекциям (1-3 материала на лекцию)
        lecture_material_data = []
        for lid in lecture_ids:
            for mi in range(random.randint(1, 3)):
                lmid = str(uuid.uuid4())
                lecture_material_ids.append(lmid)
                template = random.choice(LECTURE_CONTENT_TEMPLATES)
                topic_words = ["микропроцессоров", "баз данных", "нейронных сетей", "криптографии",
                              "телекоммуникаций", "сигналов", "нанотехнологий", "робототехники",
                              "облачных вычислений", "кибербезопасности", "интернета вещей",
                              "машинного обучения", "распределённых систем", "веб-технологий",
                              "графовых баз данных", "микросервисов", "контейнеризации",
                              "CI/CD пайплайнов", "мониторинга", "блокчейна"]
                content = template.format(topic=random.choice(topic_words))
                lecture_material_data.append((lmid, lid, "text", f"Материал к лекции", content, None, None))

        for i in range(0, len(lecture_material_data), batch_size):
            execute_values(cur,
                "INSERT INTO lecture_material (id, lecture_id, content_type, title, content_text, file_url, metadata) VALUES %s",
                lecture_material_data[i:i+batch_size]
            )

        # --- Вставка учебных групп student_group (~30-34 групп, FK → speciality) ---
        num_groups = max(30, student_count // 30)
        group_data = []
        for gi in range(num_groups):
            gid = str(uuid.uuid4())
            group_ids.append(gid)
            spec_idx = gi % len(speciality_ids)
            enrollment_year = random.choice([2022, 2023, 2024, 2025])
            group_name = f"Группа-{gi+1:03d}"
            curator = random.choice(TEACHER_NAMES)
            group_data.append((gid, speciality_ids[spec_idx], group_name, enrollment_year, curator))

        execute_values(cur,
            "INSERT INTO student_group (id, speciality_id, name, enrollment_year, curator) VALUES %s",
            group_data
        )

        # --- Вставка студентов student (batch по 500, FK → student_group) ---
        student_data = []
        for si in range(student_count):
            sid = str(uuid.uuid4())
            student_ids.append(sid)
            gid = group_ids[si % num_groups]
            is_female = random.random() < 0.45
            if is_female:
                fn = random.choice(FIRST_NAMES_F)
                ln = random.choice(LAST_NAMES_F)
                pat = random.choice(PATRONYMICS_F)
            else:
                fn = random.choice(FIRST_NAMES_M)
                ln = random.choice(LAST_NAMES_M)
                pat = random.choice(PATRONYMICS_M)
            email = f"{ln.lower()}.{fn.lower()}@mirea.ru"
            phone = f"+7-9{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(10,99)}"
            card = f"М{random.randint(100000, 999999)}"
            enroll_date = date(random.choice([2022, 2023, 2024, 2025]), 9, 1)
            status = random.choices(["active", "academic_leave", "expelled"], weights=[0.9, 0.05, 0.05])[0]
            student_data.append((sid, gid, fn, ln, pat, email, phone, card, enroll_date, status))

        for i in range(0, len(student_data), batch_size):
            execute_values(cur,
                "INSERT INTO student (id, group_id, first_name, last_name, patronymic, email, phone, student_card_number, enrollment_date, status) VALUES %s",
                student_data[i:i+batch_size]
            )

        # --- Вставка расписания schedule (batch по 500, FK → lecture, student_group) ---
        # Генерация расписания (семестр 1-4: осень 2025, семестр 5-8: весна 2026)
        schedule_data = []
        fall_start = date(2025, 9, 1)
        spring_start = date(2026, 2, 9)

        cur.execute("SELECT id, semester FROM lecture_course")
        course_semester_map = {str(r[0]): r[1] for r in cur.fetchall()}

        for gid_idx, gid in enumerate(group_ids):
            spec_id = group_data[gid_idx][1]
            group_course_ids = spec_courses.get(spec_id, [])

            for cid in group_course_ids[:8]:
                ci = course_to_idx.get(cid)
                if ci is None:
                    continue
                course_lectures = [lecture_ids[ci * num_lectures_per_course + li] for li in range(num_lectures_per_course) if ci * num_lectures_per_course + li < len(lecture_ids)]

                semester = course_semester_map.get(cid, 1)

                if semester <= 4:
                    base_date = fall_start
                else:
                    base_date = spring_start

                semester_weeks = 17
                for li, lid in enumerate(course_lectures):
                    if semester <= 4:
                        week_num = li % semester_weeks
                        day_in_week = random.randint(0, 5)
                        sd = fall_start + timedelta(weeks=week_num, days=day_in_week)
                    else:
                        week_num = li % semester_weeks
                        day_in_week = random.randint(0, 5)
                        sd = spring_start + timedelta(weeks=week_num, days=day_in_week)

                    if sd.year == 2025 and sd.month > 12:
                        continue
                    if sd.year == 2026 and sd.month > 6:
                        continue

                    ws = sd - timedelta(days=sd.weekday())
                    start_h = random.choice([8, 9, 10, 11, 12, 13, 14, 15])
                    end_h = start_h + 1 + (1 if start_h < 14 else 0)
                    classroom = random.choice(CLASSROOMS)
                    teacher = random.choice(TEACHER_NAMES)
                    sched_id = str(uuid.uuid4())
                    schedule_ids.append(sched_id)
                    schedule_data.append((
                        sched_id, lid, gid, sd, ws,
                        time(start_h, 0), time(start_h + 1, 30),
                        classroom, teacher, "scheduled"
                    ))

        for i in range(0, len(schedule_data), batch_size):
            execute_values(cur,
                "INSERT INTO schedule (id, lecture_id, group_id, scheduled_date, week_start_date, start_time, end_time, classroom, teacher_name, status) VALUES %s",
                schedule_data[i:i+batch_size]
            )

        # --- Вставка посещаемости attendance (партиционированная таблица, batch по 500;
        #     каждая запись: студент присутствовал/отсутствовал на занятии, FK → schedule, student) ---
        # Генерация посещаемости (для каждого студента в группе создаётся запись с is_present=True/False)
        logger.info("Generating attendance records...")
        cur.execute("SELECT id, group_id, week_start_date FROM schedule")
        schedule_info = cur.fetchall()

        group_students = {}
        for i, sd_row in enumerate(student_data):
            gid = sd_row[1]
            sid = student_ids[i]
            group_students.setdefault(gid, []).append(sid)

        attendance_data = []
        for sched_row in schedule_info:
            sched_id = str(sched_row[0])
            sched_gid = str(sched_row[1])
            ws = sched_row[2]
            students_in_group = group_students.get(sched_gid, [])
            if not students_in_group:
                continue
            attendance_rate = random.uniform(0.5, 0.95)
            num_attending = max(1, int(len(students_in_group) * attendance_rate))
            attending = set(random.sample(students_in_group, min(num_attending, len(students_in_group))))

            for sid in students_in_group:
                is_present_val = sid in attending
                marked = datetime(ws.year, ws.month, ws.day, random.randint(8, 18), random.randint(0, 59))
                attendance_data.append((str(uuid.uuid4()), sched_id, sid, ws, is_present_val, marked))

        for i in range(0, len(attendance_data), batch_size):
            execute_values(cur,
                "INSERT INTO attendance (id, schedule_id, student_id, week_start_date, is_present, marked_at) VALUES %s",
                attendance_data[i:i+batch_size]
            )

        pg.commit()
        logger.info("PostgreSQL data committed")

    except Exception as e:
        pg.rollback()
        logger.error(f"PostgreSQL error: {e}")
        raise
    finally:
        cur.close()
        pg.close()

    logger.info("PostgreSQL data committed")

    counts = {
        "university": 1,
        "institutes": len(institute_ids),
        "departments": len(department_ids),
        "specialities": len(speciality_ids),
        "department_specialities": len(dept_spec_ids),
        "lecture_courses": len(course_ids),
        "lectures": len(lecture_ids),
        "lecture_materials": len(lecture_material_ids),
        "student_groups": len(group_ids),
        "students": len(student_ids),
        "schedule": len(schedule_ids),
        "attendance": len(attendance_data),
    }

    logger.info(f"Data generation complete: {counts}")
    return {"status": "success", "counts": counts}


def clear_postgres():
    """
    Очистка PostgreSQL (все таблицы в обратном порядке FK).
    Остальные СУБД (Elasticsearch, Neo4j, Redis, MongoDB) очищаются автоматически
    при повторной генерации — CDC pipeline удалит старые данные и запишет новые.
    ES индекс 'lectures' пересоздаётся Lab1 при старте (ensure_lectures_index).
    """
    try:
        pg = get_pg_conn()
        cur = pg.cursor()
        cur.execute("SET CONSTRAINTS ALL DEFERRED")
        tables = [
            "attendance", "schedule", "student", "student_group",
            "lecture_material", "lecture", "lecture_course",
            "department_specialities", "speciality", "department",
            "institute", "university"
        ]
        for t in tables:
            cur.execute(f"TRUNCATE TABLE {t} CASCADE")
        pg.commit()
        cur.close()
        pg.close()
        logger.info("PostgreSQL cleared")
        return {"status": "cleared"}
    except Exception as e:
        logger.error(f"PostgreSQL clear error: {e}")
        return {"status": "error", "message": str(e)}


def get_status():
    """
    Проверка состояния данных: подсчёт студентов и курсов в PostgreSQL.
    Если студент > 0 — статус «ready», иначе «empty».
    """
    try:
        pg = get_pg_conn()
        cur = pg.cursor()
        cur.execute("SELECT COUNT(*) FROM student")
        student_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM lecture_course")
        course_count = cur.fetchone()[0]
        cur.close()
        pg.close()
        return {"students": student_count, "courses": course_count, "status": "ready" if student_count > 0 else "empty"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def list_groups():
    """
    Возвращает список групп (id, name, enrollment_year) из PostgreSQL.
    Используется в Lab3 для заполнения выпадающего списка group_name.
    """
    try:
        pg = get_pg_conn()
        cur = pg.cursor()
        cur.execute("SELECT id, name, enrollment_year FROM student_group ORDER BY name")
        groups = [{"id": str(r[0]), "name": r[1], "enrollment_year": r[2]} for r in cur.fetchall()]
        cur.close()
        pg.close()
        return {"groups": groups}
    except Exception as e:
        return {"groups": [], "error": str(e)}
