FROM bitnami/spark:3.1.3

# Install pip and required packages
USER root
#RUN apt-get update && apt-get install -y python3-pip

# Copy your requirements file
COPY requirements.txt /opt/bitnami/spark/requirements.txt
COPY pipeline.pkl /tmp/pipeline.pkl
COPY last.py /last.py
COPY TextCleaner.py /TextCleaner.py

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt

USER 1001
