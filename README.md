# 📊 데이터 파이프라인 구축 프로젝트

## 📌 프로젝트 소개

공공데이터 API를 활용하여 데이터를 수집하고,
자동화된 데이터 파이프라인을 통해 정제 및 저장까지 수행하는 프로젝트입니다.

이 프로젝트는 데이터 엔지니어링의 핵심 과정인
**데이터 수집 → 처리 → 저장 → 자동화**를 직접 구현하는 것을 목표로 합니다.

---

## 🏗 아키텍처

```
[공공데이터 API]
        ↓
[Airflow DAG (스케줄링)]
        ↓
[Raw 데이터 저장]
        ↓
[데이터 정제 (Python)]
        ↓
[MySQL 저장]
```

---

## 🔄 데이터 흐름

1. 공공데이터 API를 통해 데이터를 주기적으로 수집
2. 수집된 데이터를 Raw 형태로 저장
3. Pandas를 활용하여 데이터 정제 및 변환
4. 정제된 데이터를 MySQL에 저장
5. Airflow를 통해 전체 과정 자동화

---

## 🛠 사용 기술

* Python
* Apache Airflow
* MySQL
* Docker
* Pandas

---

## 🚀 실행 방법

### 1. Docker로 MySQL 실행

```bash
docker run -d \
  --name mysql-container \
  -e MYSQL_ROOT_PASSWORD=1234 \
  -p 3306:3306 \
  mysql:8
```

### 2. Python 가상환경 생성 및 활성화

```bash
python -m venv airflow_env
source airflow_env/bin/activate  # macOS / Linux
# airflow_env\Scripts\activate   # Windows
```

### 3. Airflow 설치

```bash
pip install apache-airflow
```

### 4. Airflow 실행

```bash
airflow db init
airflow webserver
airflow scheduler
```

### 5. DAG 실행

* Airflow UI 접속 후 DAG 활성화

---

## 📁 프로젝트 구조

```
project/
 ├─ dags/
 │    └─ data_pipeline.py
 ├─ scripts/
 │    ├─ extract.py
 │    ├─ transform.py
 │    └─ load.py
 ├─ data/
 ├─ docker/
 └─ README.md
```

---

## ⚠️ 트러블슈팅

### 1. Airflow 설치 시 dependency 충돌

* 문제: 패키지 버전 충돌 발생
* 해결: Python 가상환경 분리

### 2. MySQL 연결 실패

* 문제: 컨테이너 실행 후 접속 불가
* 해결: 포트(3306) 및 계정 정보 확인

---

## 💡 개선 방향

* Apache Spark를 활용한 대용량 데이터 처리
* Docker Compose를 활용한 전체 환경 통합
* 데이터 시각화 대시보드 추가 (Streamlit)

---

## 🎯 프로젝트 목표

* 데이터 파이프라인 전체 흐름 이해
* Airflow 기반 자동화 경험
* 실무형 데이터 엔지니어링 역량 강화
