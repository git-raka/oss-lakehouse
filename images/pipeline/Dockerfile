FROM python:3.12-slim-bookworm

# Java for Spark, Tesseract OCR (English + Indonesian language packs), Poppler (PDF -> image)
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless tesseract-ocr tesseract-ocr-ind tesseract-ocr-eng \
      poppler-utils fonts-dejavu-core curl procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Delta Lake + S3A connector jars (Hadoop 3.5.0 = the version bundled with PySpark 4.2.0)
ARG MVN=https://repo1.maven.org/maven2
RUN cd "$(python -c 'import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')" \
 && curl -fsSLO $MVN/io/delta/delta-spark_4.2_2.13/4.4.1/delta-spark_4.2_2.13-4.4.1.jar \
 && curl -fsSLO $MVN/io/delta/delta-storage/4.4.1/delta-storage-4.4.1.jar \
 && curl -fsSLO $MVN/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
 && curl -fsSLO $MVN/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
 && curl -fsSLO $MVN/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar

ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
WORKDIR /app
