FROM python:3.11-slim

WORKDIR /app

COPY redis-cdc-delete/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY redis-cdc-delete/app.py .

CMD ["python", "-u", "app.py"]
