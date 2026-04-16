from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import requests
from urllib.parse import unquote
import time
import random
import os

#KAFKA_BROKER = "localhost:9092"
KAFKA_BROKER = "kafka1:19092"
#SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "bus_raw_data"

# Schema Registry 설정
schema_registry_conf = {'url' : SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

base_dir = os.path.dirname(os.path.abspath(__file__))
schema_path = os.path.join(base_dir, "schemas", "bus_schema.avsc")

# Avro 스키마 로드
with open(schema_path, encoding="utf-8") as f :
    schema_str = f.read()

# Python dict를 Avro 형식으로 그대로 넘기기 위한 변환 함수
def dict_to_avro(obj, ctx):
    return obj

avro_serializer = AvroSerializer(schema_registry_client, schema_str, dict_to_avro)

def key_serializer(key, ctx) :
    return key.encode('utf-8')

# Producer 설정
producer_conf = {
    'bootstrap.servers' : KAFKA_BROKER,
    'key.serializer' : key_serializer,
    'value.serializer' : avro_serializer
}

producer = SerializingProducer(producer_conf)

# delivery Callback
def delivery_report(err, msg) :
    if err is not None:
        print(f"Delivery failed : {err}")
    else :
        print(
            f"✅ Delivered topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={msg.key()}"
        ) # kafka offset은 kafka broker가 자동으로 생성하고 관리함, consumer가 읽을 때 offset 확인

# 데이터 생성
def fetch_data() :
    ''' api 되면 api로 받아오도록 수정할 것
    url = 'http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRoute'
    api_key = '3dc8176e69cd6b7ffc8a23753a645fa283440ed08e72bc5c85d9e897f663b3d7'
    api_key_decode = requests.utils.unquote(api_key)
    params = {
        #'serviceKey' : '3dc8176e69cd6b7ffc8a23753a645fa283440ed08e72bc5c85d9e897f663b3d7',
        'serviceKey' : api_key_decode,
        'stId' : '112000001',
        'busRouteId' : '100100118',
        'ord' : '18'
    }

    response = requests.get(url, params=params) #######여기서부터.....
    api_json = response.json()
    #필요한 필드만 추출해서 dict로 변환
    data = {
        "station_id": api_json["stId"],
        "station_name": api_json["stNm"],
        "bus_id": api_json["busRouteId"],
        "arrival_sec_1": parse_arrmsg(api_json["arrmsg1"]),
        "arrival_sec_2": parse_arrmsg(api_json["arrmsg2"]),
        "event_time": int(time.time() * 1000),
        "ingestion_time": int(time.time() * 1000)
    }
    return data
    '''
    #임시 데이터
    return {
        "station_id": "112000001",
        "station_name": "강남역",
        "bus_id": str(random.randint(1000, 2000)),
        "arrival_sec_1": random.randint(30, 600),
        "arrival_sec_2": random.randint(600, 1200),
        "ingestion_time": int(time.time() * 1000)
    }

# kafka 전송 함수
def send_to_kafka(data) :
    try:
        producer.produce(
            topic=TOPIC,
            key=data["bus_id"], #partition key
            value=data,
            on_delivery=delivery_report #kafka producer는 비동기 방식이기 때문에 메시지 전송 결과를 확인하기 위해 사용
        )
        producer.poll(0) #내부 이벤트(전송 결과, callback 등)를 처리해주는 함수, 전송 결과(callback)를 처리하려면 필요
    except Exception as e:
        print(f"Kafka produce error: {e}")


# 메인 루프
def run() :
    try:
        while True : 
            data = fetch_data()
            print("Produced Data:", data)
            send_to_kafka(data)
        #producer.poll(0) #내부 이벤트(전송 결과, callback 등)를 처리해주는 함수, 전송 결과(callback)를 처리하려면 필요
        #producer.flush() #버퍼에 남아있는 메시지를 전부 kafka로 강제로 전송하고 끝날 때까지 기다리는 함수. 실시간이면 필요 없는듯?
            time.sleep(5) # 5초마다 전송
    except KeyboardInterrupt: #프로그램 강제 종료 방지, 정상 종료 흐름으로 넘김 (없으면 : traceback 떠서 지저분함)
        pass 
    finally:
        producer.flush() #kafka로 아직 안 보내진 메시지 전부 전송 (데이터 유실 방지)

if __name__ == "__main__":
    run()
    #data = fetch_data()
    #send_to_kafka(data)