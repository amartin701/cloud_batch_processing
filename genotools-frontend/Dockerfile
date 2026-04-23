FROM python:3.11-slim

WORKDIR /app

RUN pip install flask pika requests prometheus-client werkzeug pymongo

COPY app.py /app/
COPY templates/ /app/templates/

RUN mkdir -p /shared/uploads /shared/results

EXPOSE 5000

CMD ["python", "app.py"]