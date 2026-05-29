from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time
import random
import requests
from urllib.parse import unquote
import os
from datetime import datetime
from dotenv import load_dotenv
import re



# Kafka / Schema Registry
KAFKA_BROKER = "kafka1:19092,kafka2:19093,kafka3:19094"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "bus_raw_data"


# Schema Registry Client
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)


# Avro Schema load
base_dir = os.path.dirname(os.path.abspath(__file__))
schema_path = os.path.join(base_dir, "schemas", "bus_schema.avsc")

with open(schema_path, "r", encoding="utf-8") as f:
    schema_str = f.read()


# Avro serializer
def to_dict(obj, ctx):
    return obj

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    to_dict
)


# Producer config
producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "key.serializer": lambda k, ctx: k.encode("utf-8"),
    "value.serializer": avro_serializer
}

producer = SerializingProducer(producer_conf)


# Delivery callback
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
        )


# 임시 Data
'''
def fetch_data():
    return {
        "station_id": "112000001",
        "station_name": "강남역",
        "bus_id": "1000",
        "arrival_sec_1": random.randint(30, 600),
        "arrival_sec_2": random.randint(600, 1200),
        "mkTm": int(time.time() * 1000), # mktm을 event_time으로 쓸 것
        "ingestion_time": int(time.time() * 1000)
    }
'''

# arrival 파싱
def extract_seconds(msg):
    if not msg:
        return None

    if "곧" in msg:
        return 0

    min_match = re.search(r'(\d+)분', msg)
    sec_match = re.search(r'(\d+)초', msg)

    minutes = int(min_match.group(1)) if min_match else 0
    seconds = int(sec_match.group(1)) if sec_match else 0

    if minutes == 0 and seconds == 0:
        return None

    return minutes * 60 + seconds


# 데이터 생성
# schema.avsc에서 arrival_sec_1,2의 default를 null로 해놓은 이유 : 곧 도착 대응
def fetch_data() :
    try :

        #임시 데이터
        data = {
            "station_id": "112000001",
            "station_name": "강남역",
            "bus_id": '8780',
            "arrival_sec_1": random.randint(30, 600),
            "arrival_sec_2": random.randint(600, 1200),
            "event_time": int(datetime.now().timestamp() * 1000)
        }
        return data
        

        '''
        #api 일일 제한으로 주석처리 (성공 확인함)
        url = 'http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRoute'
        
        # 로컬에서만 .env 로드
        if os.getenv("ENV") != "prod":
            load_dotenv()

        api_key = os.getenv("BUS_API_KEY")

        params = {
            'serviceKey' : api_key,
            'stId' : '121000010',
            'busRouteId' : '100100596',
            'ord' : '11',
            'resultType' : 'json'
        }
        response = requests.get(url, params=params) 

        if response.status_code != 200:
            print("API 실패:", response.status_code)
            return None

        api_json = response.json()
        items = api_json.get("msgBody", {}).get("itemList", [])

        if not items:
            print("itemList 없음")
            return None
        
        item = items[0]

        # python datetime을 kafka에 그대로 전달 불가 => timestamp(long) 형식 사용
        dt = datetime.strptime(item["mkTm"], "%Y-%m-%d %H:%M:%S.%f")

        #필요한 필드만 추출해서 dict로 변환        
        data = {
            "station_id": item["stId"],
            "station_name": item["stNm"],
            "bus_id": item["rtNm"],
            "arrival_sec_1": extract_seconds(item["arrmsg1"]),
            "arrival_sec_2": extract_seconds(item["arrmsg2"]),
            "event_time": int(dt.timestamp() * 1000)
        }
           

        return data
        '''
    except Exception as e:
        print(f"JSON 파싱 실패: {e}")


# Send to Kafka
def send_to_kafka(data):
    try:
        producer.produce(
            topic=TOPIC,
            key=data["bus_id"],
            value=data,
            on_delivery=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        print(f"Kafka produce error: {e}")


def run():
    try:
        while True:
            data = fetch_data()
            print("Produced:", data)
            if data:
                send_to_kafka(data)
            time.sleep(5)

    except KeyboardInterrupt:
        print("Stopped by user")

    finally:
        producer.flush()

if __name__ == "__main__":
    run()