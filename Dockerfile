# Entorno local con las mismas versiones principales que Microsoft Fabric Runtime 1.3:
# Apache Spark 3.5, Delta Lake 3.2 y Python 3.11.
ARG PYTHON_IMAGE=python:3.11-slim-bookworm
FROM ${PYTHON_IMAGE}

ARG DELTA_VERSION=3.2.1
ARG SCALA_VERSION=2.12

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl procps git \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Los JAR de Delta se incluyen en la imagen para que Spark no los descargue en cada sesión.
RUN SPARK_JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && for artifact in "delta-spark_${SCALA_VERSION}" "delta-storage"; do \
         curl -fsSL -o "${SPARK_JARS}/${artifact}-${DELTA_VERSION}.jar" \
           "https://repo1.maven.org/maven2/io/delta/${artifact}/${DELTA_VERSION}/${artifact}-${DELTA_VERSION}.jar"; \
       done

ENV PYTHONPATH=/workspace

CMD ["bash"]
