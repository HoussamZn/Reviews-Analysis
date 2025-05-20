FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .
COPY test_data.json .
ENV PYTHONUNBUFFERED=1
CMD ["python", "producer.py"]
