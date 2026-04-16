FROM python:3.10-slim

WORKDIR /app

COPY python_app/ /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "fetch_bus_api.py"]