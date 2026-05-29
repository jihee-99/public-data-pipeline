FROM apache/spark:3.5.1 AS builder

USER root

RUN /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.spark:spark-avro_2.12:3.5.1,\
mysql:mysql-connector-java:8.0.33,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar

RUN mkdir -p /ivy \
 && cp -r /root/.ivy2/jars /ivy/jars


FROM apache/spark:3.5.1

USER root
WORKDIR /app

########추가
COPY spark/conf/core-site.xml /opt/spark/conf/core-site.xml

COPY spark/process_spark.py /app/process_spark.py

COPY python_app /app/python_app

# dependency 고정
COPY --from=builder /ivy/jars /opt/spark/jars

COPY spark/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

RUN chown -R spark:spark /app

USER spark