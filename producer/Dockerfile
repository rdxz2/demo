FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY producer.py /app/

CMD ["python", "producer.py"]
