# Follow Ecommerce

이커머스 핵심 도메인을 구현하고, **멀티 모듈 구조와 이벤트 기반 비동기 처리를 통해 상품 지표·랭킹을 분리한 백엔드 시스템**입니다.

회원, 포인트, 상품, 좋아요, 주문, 재고, 결제 도메인을 구현하고
트래픽이 집중되는 조회 영역과 비동기 집계 영역을 별도의 애플리케이션으로 분리했습니다.

---

## Tech Stack

* **Java 21**
* **Spring Boot**
* **Spring Data JPA / Hibernate**
* **MySQL**
* **Redis**
* **Kafka**
* **Spring Batch**
* **JUnit 5 / Mockito**
* **Testcontainers**
* **K6**
* **Gradle Kotlin DSL**
* **Docker**

---

## Architecture

애플리케이션과 공통 인프라를 멀티 모듈로 분리했습니다.

```text
Root
├── apps
│   ├── commerce-api
│   ├── commerce-batch
│   └── commerce-streamer
│
├── modules
│   ├── jpa
│   ├── redis
│   └── kafka
│
└── supports
    ├── jackson
    ├── logging
    └── monitoring
```

* `commerce-api` : 이커머스 핵심 API
* `commerce-streamer` : Kafka 이벤트 소비 및 비동기 집계
* `commerce-batch` : 주간/월간 랭킹 집계
* `modules` : JPA / Redis / Kafka 공통 구성
* `supports` : Logging / Monitoring 등 공통 부가 기능

---

## Domain

```text
User
 ├── Point
 └── Like

Brand
 └── Product
      ├── Option
      └── SKU

User
 └── Order
      ├── Order Item
      └── Payment
```

주요 기능:

* 회원가입 / 회원 조회
* 포인트 충전 / 사용
* 브랜드 / 상품 조회
* 상품 옵션 / SKU / 재고 관리
* 상품 좋아요
* 주문 / 주문 취소
* 결제 / 결제 취소

[요구사항](./docs/design/01-requirements.md) · [ERD](./docs/design/04-erd.md)

---

## Engineering Focus

### 1. 멀티 모듈 구조

**실행 애플리케이션과 공통 인프라를 어떻게 분리할 것인가?**

API, 이벤트 처리, 배치 애플리케이션을 분리하고 JPA, Redis, Kafka와 같은 공통 인프라를 재사용 가능한 모듈로 구성했습니다.

각 애플리케이션이 필요한 모듈만 의존하도록 구성하여 애플리케이션 간 결합도를 낮췄습니다.

→ [프로젝트 구조](./README.md)

---

### 2. Kafka 기반 이벤트 처리

**조회·좋아요·재고 이벤트를 동기 API 처리와 분리할 수 있는가?**

상품 조회, 좋아요, 재고 관련 이벤트를 Kafka로 발행하고 `commerce-streamer`에서 Batch Consumer로 처리하도록 구성했습니다.

```text
commerce-api
      │
      │ Event
      ▼
    Kafka
      │
      ▼
commerce-streamer
      │
      ├── 상품 지표 집계
      ├── 랭킹 갱신
      └── 이벤트 감사 로그
```

Consumer는 이벤트의 `event_id`를 기준으로 처리 여부를 확인하여 **동일 이벤트의 중복 처리를 방지**하도록 구성했습니다.

→ [Kafka Consumer](./apps/commerce-streamer/src/main/java/com/loopers/interfaces/consumer)

---

### 3. 랭킹 집계

**실시간 요청에서 랭킹 계산까지 수행해야 하는가?**

상품 조회·좋아요 등의 이벤트를 실시간 API에서 직접 집계하지 않고 이벤트 스트림을 통해 지표를 수집한 뒤, 주간/월간 랭킹을 배치로 계산하도록 분리했습니다.

```text
Event
  ↓
Kafka
  ↓
Streamer
  ↓
Product Metrics
  ↓
Spring Batch
  ↓
Weekly / Monthly Ranking
  ↓
Redis Sorted Set
```

Spring Batch는 **chunk 1,000 단위**로 데이터를 처리하고, 계산된 랭킹을 DB와 Redis에 함께 반영합니다.

Redis에는 Sorted Set을 사용하여 랭킹 조회에 적합한 형태로 저장하고 Top-K 데이터만 유지하도록 구성했습니다.

→ [Weekly Ranking Job](./apps/commerce-batch/src/main/java/com/loopers/batch/job/WeeklyRankingJobConfig.java)
→ [Monthly Ranking Job](./apps/commerce-batch/src/main/java/com/loopers/batch/job/MonthlyRankingJobConfig.java)

---

### 4. 이벤트 멱등성

**Kafka Consumer에서 동일 이벤트가 다시 전달되면 어떻게 할 것인가?**

Kafka Consumer에서 `event_id`를 확인하고 이미 처리된 이벤트인지 검증한 후 실제 집계를 수행하도록 구성했습니다.

이를 통해 Consumer 재처리 상황에서도 상품 지표나 감사 로그가 중복 반영되는 것을 방지했습니다.

→ [Event Handling](./apps/commerce-streamer/src/main/java/com/loopers/domain/eventhandle)

---

### 5. 상품 조회 성능

**상품 목록과 상세 조회가 증가하는 상황에서 어느 수준의 성능을 유지할 것인가?**

K6를 이용해 상품 목록 및 상세 조회에 대한 부하 테스트 시나리오를 구성했습니다.

상품 상세 조회는 최대 **1,000 VU**까지 부하를 증가시키며 p95 응답시간 threshold를 설정했고, 상품 목록 조회 역시 페이지·브랜드·정렬 조건을 변화시키며 테스트하도록 구성했습니다.

→ [K6 Performance Test](./performance/k6)

---

## Test

* Unit Test
* Integration Test
* Kafka Consumer Test
* Testcontainers
* K6 Load Test

---

## Documentation

* [요구사항](./docs/design/01-requirements.md)
* [Sequence Diagram](./docs/design/02-sequence-diagrams.md)
* [Class Diagram](./docs/design/03-class-diagrams.md)
* [ERD](./docs/design/04-erd.md)
* [K6 Performance Test](./performance/k6)

---

## Run

### Requirements

* JDK 21
* Docker

### Test

```bash
./gradlew test
```

### Build

```bash
./gradlew build
```
