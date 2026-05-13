from pyspark.sql import SparkSession
import requests
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, current_timestamp, window, avg, first, max, round
import pymysql
from pyspark.sql.functions import to_date


spark = SparkSession.builder \
    .appName("bus-streaming") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    ) \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# kafka source
# .option("failOnDataLoss", "false") : offset 날아가도 job 안 죽음 => kafka에 예전 offset 없으면 그냥 건너뛰고 계속 진행
# s3는 언제 쓰냐? => kafka 데이터 날아감 ->  kafka로는 복구 불가능, 대신 s3 있으면 다시 처리 가능
# 그전까지 테스트용 : 컨테이너 내리고 -> vscode 터미널에 Remove-Item -Recurse -Force .\chk -> 컨테이너 올리기
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "bus_raw_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

'''
# key, value test
debug_query = kafka_df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)"
).writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()
'''

# schema registry
SCHEMA_REGISTRY = "http://schema-registry:8081"
TOPIC = "bus_raw_data"

schema_url = f"{SCHEMA_REGISTRY}/subjects/{TOPIC}-value/versions/latest"

schema = requests.get(schema_url).json()["schema"]

#test
print(f"Delivered success: {schema}")


# avro deserialize
df = kafka_df.select(
    from_avro(
        expr("substring(value, 6, length(value)-5)"),
        schema  # JSON Avro schema
    ).alias("data")
).select("data.*")


# dedup 중복 제거 + watermark
# watermark 역할 : 이 시간 이전 데이터는 이제 중복 체크 안 함
# ex) event_time_ts < current_time - 5min → 버림
# watermark 없으면 state 무한 증가 위험 -> spark가 모든 key를 계속 기억함, 메모리 계속 증가
# 같은 DataFrame lineage 안에서 watermark는 한 번만 정의해야 함
# =========================
df = df.withWatermark("event_time", "5 minutes") \
    .dropDuplicates(["bus_id", "event_time"])


# .trigger(processingTime="10 seconds") => 10초마다 최대한 실행함을 의미(정확히 10초마다 실행이 아님)
query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()


# lastupdate 생성 => 데이터가 실제로 들어간 시간
# spark는 내부적으로 timestamp 컬럼만 "시간 흐름"으로 판단함
# =========================
df = df.withColumn(
    "lastupdate",
    current_timestamp()
)


############################# s3 write 로직 ##################################
#용량 제한으로 주석처리 (잘 올라가는 거 확인함)
'''
# s3 저장
# partition column 생성
df = df.withColumn(
    "dt",
    to_date(col("event_time"))
)


# raw parquet 저장 
raw_query = df.writeStream \
    .format("parquet") \
    .option(
        "path",
        "s3a://mybus-arrival-pipeline/bus/raw/"
    ) \
    .option(
        "checkpointLocation",
        "/chk/s3/raw"
    ) \
    .partitionBy("dt") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

'''

############################## s3 read 로직 ##################################
#(잘 읽혀지는 거 확인함)
'''
# 아래처럼 read하면 partition 기준 컬럼이 dataframe에 없음
df = spark.read.parquet("s3a://mybus-arrival-pipeline/bus/raw/dt=2026-05-08/")

# 아래처럼 read해야 partition column 까지 함께 dataframe에서 확인 가능
df = spark.read \
    .option("basePath", "s3a://mybus-arrival-pipeline/bus/raw/") \
    .parquet("s3a://mybus-arrival-pipeline/bus/raw/dt=2026-05-08/")

df.show(30, truncate=False)
'''



# Aggregation => 버스 도착 시간 실시간 평균
# API 데이터는 노이즈가 있음 => GPS 오차, 교통 상황 반영 지연, 예측 알고리즘 흔들림 등의 이유로 값이 계속 출렁임
# => 그래서 1분 평균을 내면 갑자기 300초 → 120초 → 250초 튀는 걸 안정화할 수 있음.
# 10초 마다 호출 -> 1분에 데이터 6개 / 30초마다 호출 -> 1분에 데이터 2개 => 이걸 평균 내면 단일 값보다 훨씬 신뢰도 높아짐
# window(col("lastupdate"), "1 minute") => 같은 시간대 데이터끼리 묶어서 통계 내기
# =========================
agg_df = df.groupBy(
    window(col("event_time"), "5 minute"),
    col("bus_id")
    ).agg(
        avg("arrival_sec_1").alias("avg_arrival1"),
        avg("arrival_sec_2").alias("avg_arrival2"),
        first("station_id", ignorenulls=True).alias("station_id"),
        first("station_name", ignorenulls=True).alias("station_name"),
        max("lastupdate").alias("lastupdate") 
    )

# select + round
result_df = agg_df.select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("bus_id"),
    col("station_id"),
    col("station_name"),
    col("lastupdate"),
    round(col("avg_arrival1"), 2).alias("avg_arrival1"),
    round(col("avg_arrival2"), 2).alias("avg_arrival2")
)

# .outputMode("append") \ : 완전히 끝난 결과만 출력
# .outputMode("update") \ : 바뀐 결과만 계속 출력
query = result_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

# mysql upsert function
def write_to_mysql(batch_df, batch_id):

    print(f"batch_id = {batch_id}")

    # empty batch skip (중요)
    if batch_df.rdd.isEmpty():
        print("empty batch skipped")
        return

    # repartition (MySQL 안전 수준)
    # partition 2개 => excutor 2개 => 각각 함수 실행
    batch_df = batch_df.repartition(2)

    print("schema:")
    batch_df.printSchema()

    def upsert_partition(partition):

        conn = None
        cursor = None

        sql = """
        INSERT INTO bus_arrival (
            start_time, end_time, bus_id,
            station_id, station_name,
            avg_arrival1, avg_arrival2, lastupdate
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            avg_arrival1 = VALUES(avg_arrival1),
            avg_arrival2 = VALUES(avg_arrival2),
            station_id = VALUES(station_id),
            station_name = VALUES(station_name),
            lastupdate = VALUES(lastupdate)
        """

        batch = []

        try:
            
            conn = pymysql.connect(
                host="mysql",
                user="root",
                password="wlgml3574@#",
                database="bus",
                autocommit=False,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30
            )

            cursor = conn.cursor()

            for row in partition:

                print("rows:", row)
                batch.append((
                    row.start_time,
                    row.end_time,
                    row.bus_id,
                    row.station_id,
                    row.station_name,
                    float(row.avg_arrival1) if row.avg_arrival1 is not None else None,
                    float(row.avg_arrival2) if row.avg_arrival2 is not None else None,
                    row.lastupdate
                ))

                print("after sample rows:")

                # batch size (MySQL safe range)
                if len(batch) >= 200:
                    print("executing batch insert:", len(batch))
                    cursor.executemany(sql, batch)
                    conn.commit()
                    batch.clear()

            # remaining batch flush
            if batch:
                print("final flush:", len(batch))
                print("batch data:", batch)
                cursor.executemany(sql, batch)
                conn.commit()

        except Exception as e:
            print(f"partition error: {e}")
            if conn:
                conn.rollback()

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # execute in parallel partitions
    batch_df.foreachPartition(upsert_partition)  # 병렬 처리

# streaming query
query = result_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("update") \
    .option("checkpointLocation", "/chk/mysql/") \
    .trigger(processingTime="10 seconds") \
    .start()

# S3 → 원본 + 재처리용 (필수) => S3 raw 저장 설계 (partition 포함)
# MySQL → 가공 + 빠른 조회 (필수) => overwrite/upsert 가능하게
# Dashboard → MySQL 기반 시각화
# airflow로 자동화까지 하기.

# 재처리 가능한 S3 partition 설계 + Spark backfill 코드 구조 물어보기


spark.streams.awaitAnyTermination()