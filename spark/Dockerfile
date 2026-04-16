FROM apache/spark:3.5.1

USER root

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV SPARK_LOCAL_DIRS=/tmp/spark

WORKDIR /app

# 앱 코드
COPY process_spark.py /app/process_spark.py

# 🔥 Kafka connector JAR 미리 다운로드
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar /opt/spark/jars/

# (필요하면 추가 dependency 여기에 더 넣음)

RUN mkdir -p /home/spark /tmp/spark \
 && chown -R spark:spark /app /tmp /home/spark /opt/spark

USER spark