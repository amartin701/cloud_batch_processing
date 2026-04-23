FROM python:3.11-slim

WORKDIR /app

RUN pip install flask kubernetes pika prometheus-client requests

COPY genotools-job-controller.py /app/

EXPOSE 8080

CMD ["python", "genotools-job-controller.py"]