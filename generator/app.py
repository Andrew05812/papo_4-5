"""
Модуль app.py — тонкий FastAPI-фасад для сервиса генерации тестовых данных.

Генератор заполняет ТОЛЬКО PostgreSQL (остальные БД заполняются через CDC:
Debezium → Kafka → Sink Connectors → ES/Neo4j/Redis/MongoDB).
Каждый эндпоинт делегирует вызов в модуль generator.py.
"""
from fastapi import FastAPI
import generator

app = FastAPI(title="Data Generator Service")

# POST /generate — создаёт тестовые данные в PostgreSQL (CDC разнесёт по остальным БД)
@app.post("/generate")
def generate_data():
    return generator.generate_data()

# DELETE /clear — очищает PostgreSQL (CDC удалит данные из остальных БД через tombstone)
@app.delete("/clear")
def clear_data():
    return generator.clear_postgres()

# GET /status — проверяет наличие данных (ready/empty) по числу студентов в PG
@app.get("/status")
def get_status():
    return generator.get_status()

# GET /groups — возвращает список групп для выпадающего списка в Lab3
@app.get("/groups")
def list_groups():
    return generator.list_groups()

# GET / — health-check, возвращает имя и описание сервиса
@app.get("/")
def root():
    return {"service": "generator", "description": "Data generation service (PostgreSQL only, CDC for other DBs)"}
