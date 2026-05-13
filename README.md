<프로젝트 소개>

실시간 버스 도착 정보 스트리밍 데이터 파이프라인 구축

- Kafka와 Spark Structured Streaming을 활용하여
실시간 버스 도착 데이터를 수집·처리하는
스트리밍 데이터 파이프라인을 구축했습니다.

- Schema Registry 기반 Avro 직렬화를 적용하여
Producer-Consumer 간 스키마 일관성을 유지했으며,

- Spark Streaming의 watermark 및 deduplication 기능을 활용해
이벤트 타임 기반 중복 제거와 안정적인 스트리밍 처리를 구현했습니다.

- 또한 원본 데이터 보존 및 재처리를 위해 S3 Data Lake 구조를 설계하고,
실시간 조회를 위한 MySQL serving layer를 구축했습니다.

- 현재 Spark Streaming 기반 실시간 파이프라인은 정상 동작하며,
Airflow를 활용한 workflow orchestration 및 자동화 파이프라인을 추가 구현 중입니다.
Docker 기반 Airflow 환경에서 DAG scheduling 및 task dependency 구성을 진행하고 있습니다.


<기술 스택>

[Data Pipeline]
- Apache Kafka
- Spark Structured Streaming
- Schema Registry
- Avro

[Storage]
- AWS S3
- MySQL

[Infra]
- Docker Compose
- Multi-broker Kafka Cluster

[Visualization]
- Apache Superset
