from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, window, avg
from pyspark.sql.avro.functions import from_avro
import json
import logging

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bus-streaming")

# Spark Session
spark = SparkSession.builder \
    .appName("bus-streaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "mysql:mysql-connector-j:8.0.33") \
    .getOrCreate()

# 로그 출력 수준을 'WARN'으로 설정
spark.sparkContext.setLogLevel("WARN")

# Kafka Source (Consumer)
# read는 배치 처리, readStream은 계속 들어오는 데이터 처리
#.format("kafka") : Spark가 Kafka에 연결해서 데이터 가져옴
#.option("kafka.bootstrap.servers", "kafka1:19092") : 여기로 접속해서 데이터 가져옴
#.option("subscribe", "bus_raw_data") : "bus_raw_data"라는 이름의 토픽 구독
#.option("startingOffsets", "latest") : 어디서부터 읽을지 설정, latest -> 지금부터 새로 들어오는 데이터만 읽기
#.option("failOnDataLoss", "false") : 일부 데이터가 없어도 에러 없이 계속 처리 진행
#.load() : 위 설정을 실제로 실행해서 dataframe 생성
logger.info("Starting Kafka stream...")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "bus_raw_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Avro Schema 정의 (Producer와 동일해야 함)
avro_schema = {
    "type" : "record", 
    "name" : "BusArrival", 
    "namespace" : "bus.streaming", 
    "fields": [ 
        {"name" : "station_id", "type" : "string"},
        {"name" : "station_name", "type" : "string"},
        {"name" : "bus_id", "type" : "string"},

        {"name" : "arrival_sec_1", "type": "int"},
        {"name" : "arrival_sec_2", "type": "int"},

        {"name": "ingestion_time", "type": "long"}
    ]
}

# 딕셔너리 형태의 Avro 스키마를 문자열로 바꿔서 avro_schema_str에 저장
avro_schema_str = json.dumps(avro_schema)

# Avro -> DataFrame 변환
# kafka 스트리밍 데이터를 사람이 쓸 수 있는 형태로 파싱하는 단계 (kafka에서 받은 바이너리 데이터를 Avro 스키마로 해석해서 dataframe 형성)
parsed_df = kafka_df.select(
    col("key").cast("string").alias("key"), #kafka의 key는 보통 바이너리임, 이걸 문자열로 변환(cast("string")), 그리고 이름을 "key"로 유지
    from_avro(col("value"), avro_schema_str).alias("data") #value -> kafka 메시지(바이너리)임. from_avro(~) -> avro 형식으로 디코딩, avro_schema -> 데이터 구조 정의(JSON 형태 스키마). 그 결과 data라는 구조체(struct) 컬럼 생성됨
).select("data.*") #data는 struct(중첩 객체), "data.*" -> 내부 필드를 전부 꺼내서 컬럼으로 펼침. / 처리 전 : key | value (binary) -> 처리 후 : bus_id | speed | timestamp | ...

# event_time 생성 (ms -> timestamp)
# ingestion_time을 timestamp 타입의 lastupdate 컬럼으로 변환해서 추가
parsed_df = parsed_df.withColumn(
    "lastupdate",
    from_unixtime(col("ingestion_time") / 1000).cast("timestamp")
)

'''
# S3 저장 (Raw Data Lake)
# parsed_df.writeStream : 스트리밍 데이터를 어디론가 계속 보내기
#.format("parquet") : 저장 포맷 Parquet -> 컬럼 기반 저장, 압축 효율 좋음
#.option("path", "s3a://your-bucket/bus/raw/") : s3a:// -> spark에서 s3 접근할 때 사용하는 프로토콜, "your-bucket/bus/raw/" -> 데이터 쌓이는 경로
#.outputMode("append") : 새 데이터만 계속 추가, 기존 데이터는 안 건드림
#.start() : 스트리밍 실행 시작
s3_query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://your-bucket/bus/raw/") \
    .outputMode("append") \
    .start()
'''

# spark 스트리밍은 데이터가 끝없음. => spark는 언제까지 데이터를 들고 있어야 하는지 모름, 늦게 들어오는 데이터도 계속 기다리면 메모리 폭발 => watermark 필요
parsed_df = parsed_df.withWatermark("lastupdate", "5 minutes") # "event_tme" 기준으로 5분 지난 데이터는 늦게 들어와도 버려라

# 집계 (1분 단위 평균 도착시간) : 1분 단위 + 버스별로 그룹화해서 도착시간 평균을 계산하는 스트리밍 집계
# 왜 window가 필요하냐 : 스트리밍 데이터는 끝이 없음. 그냥 group by 하면 끝나지 않음 -> 시간 단위로 잘라서 집계
# 같은 1분 window 안에서는 값 게속 바뀜, 그래서 outputMode 선택 중요(append/update)
agg_df = parsed_df.groupBy(
    window(col("lastupdate"), "1 minute"), # evnet_time 기준으로 1분짜리 시간 구간(window) 생성 (실시간 데이터는 계속 들어오기 때문에 시간별로 묶어서 처리)
    col("bus_id")
).agg(
    avg("arrival_sec_1").alias("avg_arrival_sec_1"),
    avg("arrival_sec_2").alias("avg_arrival_sec_2")
) #다음/다다음 버스 도착 예상 시간을 의미
# 왜 이렇게 계산하냐 : 도착 예상 시간을 안정화 하기 위해서임. 센서/데이터는 항상 흔들리기 때문에 그대로 쓰면 이상하게 보임 ex) 10초 → 200초 → 40초 → 35초. 때문에 평균을 쓰면 ≈ 70초 (더 안정적)
# 이 데이터를 계속 쌓으면 => 실시간:현재 교통 상태 / 과거 데이터:머신러닝 학습 / 미래:도착시간 예측 모델

# window flatten
# spark에서는 window aggregation을 하면 결과가 중첩 구조(struct)로 나오는데, 이걸 RDB에 넣거나 CSV/BI 툴에서 쓰려면 중첩 구조를 평평한 테이블 형태로 바꿔야 함
agg_df = agg_df.select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("bus_id"),
    col("avg_arrival_sec_1").alias("avg_arrival1"),
    col("avg_arrival_sec_2").alias("avg_arrival2")
)

# parquet sink (data lake)
# 데이터를 parquet 파일로 계속 저장하는 '스트리밍 쓰기 설정'
# parsed_df.writeStream : 일반 write가 아니라 writeStream(계속 쓰기)를 사용
#.format("parquet") : 저장 포맷을 parquet로 지정
#.option("path", "/data/raw") : 실제 데이터 파일이 저장될 위치
#.option("checkpointLocation", "/chk/parquet/") : spark가 스트리밍 상태를 저장하는 곳 => 어디까지 읽었는지(offset), 장애 복구 위치, 중복 처리 방지 정보
#.outputMode("append") : 새로운 데이터만 계속 추가
#.start() : 실제 스트리밍 작업 시작
logger.info("Starting Parquet sink...")

#30초마다 한 번씩 작업
parquet_query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/data/raw") \
    .option("checkpointLocation", "/data/chk/parquet/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# MySQL 저장 함수 (batch write)
# spark 스트리밍 결과를 mysql에 배치 단위로 저장하는 함수
def write_to_mysql(batch_df, batch_id) :
    logger.info(f"Writing batch {batch_id} to MySQL")

    if batch_df.count() == 0 :
        return

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/bus") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "bus_arrival") \
        .option("user", "root") \
        .option("password", "wlgml3574@#") \
        .option("batchsize", 1000) \
        .mode("append") \
        .save()

# MySQL Sink
# spark 스트리밍 결과를 배치 단위로 mysql에 쓰면서 체크포인트로 상태를 저장
# 스트리밍은 데이터가 계속 들어오니까 배치 단위로 db 저장
# checkpoint 역할 : 어디까지 처리했는지 저장 => offset(kafka 위치), state(window 계산 결과), 실패 복구 정보
#.option("checkpointLocation", "s3a://your-bucket/checkpoint/mysql/") \
logger.info("Starting mysql sink...")

mysql_query = agg_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("update") \
    .option("checkpointLocation", "/data/chk/mysql/") \
    .option(processingTime="30 seconds") \
    .start()

# Streaming 유지 : 스트리밍 프로그램을 '계속 실행 상태로 유지하는 역할'
logger.info("Streaming started successfully")

spark.streams.awaitAnyTermination()


