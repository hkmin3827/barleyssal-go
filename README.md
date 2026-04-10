# 🚀 Barleyssal — Go Market Gateway

> **모의 주식 거래 플랫폼**의 실시간 시세 게이트웨이 & 주문 매칭 엔진입니다.  
> 한국투자증권(KIS) Open API와 WebSocket으로 연결해 실시간 주가 데이터를 수집하고,  
> Redis 기반 호가 큐를 사용해 주문을 매칭합니다.

<br/>

## 📌 관련 레포지토리

| 서버            | 역할                                  | 링크                                                                   |
| --------------- | ------------------------------------- | ---------------------------------------------------------------------- |
| **Spring Boot** | 메인 비즈니스 로직 API 서버           | [barleyssal-spring](https://github.com/hkmin3827/barleyssal-spring)    |
| **Go** (현재)   | 실시간 시세·호가·주문 매칭 게이트웨이 | —                                                                      |
| **React**       | 프론트엔드 SPA                        | [barleyssal-react](https://github.com/hkmin3827/barleyssal-react-vite) |


<br/>


## 🏗️ 시스템 내 역할

```
KIS WebSocket (실시간 시세)
        │
        ▼
[Go Market Gateway :4000]
        │
        ├──────────────────────────────────────────┐
        │  REST API                                      │  WebSocket (/ws)
        │  /api/chart, /api/market, /api/stocks     │  클라이언트에게 실시간 push
        ▼                                                    ▼
   [React Client]                                      [React Client]
        │
        ▼
[Spring Boot :8080]
        │  Kafka [order.created]
        ▼
[Go 주문 매칭 엔진] ──Redis 호가큐──▶ 매칭 결과
        │  Kafka [execution.event]
        ▼
[Spring Boot 체결 소비자]
```

<br/>

## 🛠️ 기술 스택

| 분류           | 기술                                          |
| -------------- | --------------------------------------------- |
| Language       | Go 1.26.1                                     |
| HTTP Framework | Echo v4                                       |
| WebSocket      | gorilla/websocket                             |
| Cache          | Redis (go-redis/v9)                           |
| Message Broker | Apache Kafka (segmentio/kafka-go)             |
| Observability  | Prometheus (`/metrics`)                       |
| Logger         | Uber Zap                                      |
| Config         | godotenv (`.env` 파일)                        |
| External API   | 한국투자증권(KIS) Open API (REST + WebSocket) |

<br/>

## 📂 프로젝트 구조

```
barleyssal-go/
├── cmd/server/
│   └── main.go               # 앱 진입점, 의존성 주입 & Graceful Shutdown
├── config/
│   ├── config.go             # 환경변수 로딩
│   └── stocks.go             # 구독 종목 리스트 (KRX 40개 종목)
├── domains/
│   ├── chart/
│   │   └── application/
│   │       └── chart_service.go    # 일봉·분봉 차트 데이터 서비스
│   ├── order_matching/
│   │   ├── application/
│   │   │   └── matching_engine.go  # Redis 호가큐 기반 주문 매칭 엔진
│   │   └── infrastructure/kafka/
│   │       ├── order_consumer.go   # Spring → Go 주문 이벤트 소비
│   │       └── execution_producer.go # 체결 결과 → Spring 발행
│   └── pnl/
│       └── application/
│           └── pnl_service.go      # 계좌 손익(PnL) 스냅샷 계산
├── interfaces/http/
│   └── handlers.go           # Echo REST/WS 핸들러 & 라우트 등록
└── shared/
    ├── infrastructure/
    │   ├── websocket/
    │   │   ├── ws_server.go   # WebSocket Hub (구독 관리, 브로드캐스트)
    │   │   └── external_market_client.go # KIS WebSocket 클라이언트
    │   └── kis_auth/
    │       └── kis_auth_service.go   # KIS 토큰 발급·갱신 스케줄러
    │   └── kis_rest/
    │       ├── kis_rest_client.go  # KIS REST API 공통 클라이언트
    ├── price/
    │   └── price_service.go  # 시세 조회·정렬·배치 (Redis 캐시)
    ├── ratelimit/
    │   └── redis_rate_limiter.go  # Redis 기반 글로벌 Rate-Limit (70req/s)
    ├── session/
    │   └── resolver.go       # Spring Session Redis에서 userId 추출
    ├── ports/
    │   └── ports.go          # 인터페이스 정의 (PriceUpdater 등)
    └── utils/
        ├── data_utils.go
        └── time_converter.go
```

<br/>

## 📋 API 기능 명세

### 📊 차트 API (`/api/chart`)

| Method | Endpoint               | 설명                                                         |
| ------ | ---------------------- | ------------------------------------------------------------ |
| `GET`  | `/period/:stockCode`   | 일·주·월·연봉 차트 데이터 조회. `?period=D\|W\|M\|Y`         |
| `GET`  | `/intraday/:stockCode` | 분봉 차트 데이터 조회. `?timeframe=1m\|5m\|10m\|30m&limit=N` |

**차트 데이터 소스**: KIS REST API 호출 결과를 Redis에 캐싱 (OHLCV TTL: 7일)

---

### 🏦 시장 API (`/api/market`)

| Method | Endpoint               | 설명                                        |
| ------ | ---------------------- | ------------------------------------------- |
| `GET`  | `/search`              | 종목 이름·코드로 검색. `?q=검색어&limit=20` |
| `GET`  | `/account/pnl/:userId` | 특정 사용자의 계좌 손익(PnL) 스냅샷 조회    |

---

### 📈 주식 API (`/api/stocks`)

| Method | Endpoint                 | 설명                                                    |
| ------ | ------------------------ | ------------------------------------------------------- |
| `GET`  | `/api/stocks`            | 전체 종목 정렬 조회. `?sort=name\|changeRate\|acmlVol`  |
| `GET`  | `/api/stocks/batch`      | 복수 종목 시세 배치 조회. `?codes=005930,000660,...`    |
| `GET`  | `/api/stocks/info/:code` | 단일 종목 상세 시세 조회 (현재가·등락률·거래량·OHLC 등) |

**응답 예시 (`/api/stocks/info/:code`)**

```json
{
  "stockCode": "005930",
  "price": 78500,
  "changeRate": 1.23,
  "prdyVrssSign": "2",
  "prdyVrss": 950,
  "stckOprc": 77600,
  "stckHgpr": 79000,
  "stckLwpr": 77500,
  "volume": 1234567,
  "acmlVol": 18234567,
  "mKopCode": "200",
  "ts": 1714123456789
}
```

---

### 🔌 WebSocket (`/ws`)

클라이언트가 WebSocket으로 연결 후 구독 메시지를 보내면, 서버가 실시간 시세를 push합니다.

**클라이언트 → 서버 메시지 타입**

| type        | 설명                                                                       |
| ----------- | -------------------------------------------------------------------------- |
| `subscribe` | 종목 구독 등록. `{ "type": "subscribe", "symbols": ["005930", "000660"] }` |
| `auth`      | 사용자 인증 (Spring Session의 userId 전달)                                 |

**서버 → 클라이언트 push 메시지 타입**

| type             | 설명                                                         |
| ---------------- | ------------------------------------------------------------ |
| `price_update`   | 실시간 체결가 업데이트                                       |
| `order_executed` | 주문 체결 결과 알림                                          |
| `pnl_update`     | 계좌 손익 변동 알림                                          |
| `home_update`    | 홈화면용 등락률 Top10 & 거래량 Top10 (5초 주기 브로드캐스트) |

---

### 🩺 헬스체크 API

| Method | Endpoint      | 설명                              |
| ------ | ------------- | --------------------------------- |
| `GET`  | `/api/health` | 서버 상태 및 Redis 연결 상태 확인 |
| `GET`  | `/metrics`    | Prometheus 메트릭 수집 엔드포인트 |

<br/>

## ⚙️ 주요 기술 구현 포인트

### 주문 매칭 엔진 (Redis 기반)

Redis Sorted Set을 호가 큐로 사용하여 가격 우선·시간 우선 원칙으로 매칭합니다.

```
orders:pending:{stockCode}:BUY   (Sorted Set, score = -limitPrice)  → 높은 가격 우선
orders:pending:{stockCode}:SELL  (Sorted Set, score = limitPrice)   → 낮은 가격 우선
order:meta:{orderId}             (Hash, 주문 메타데이터)
```

- **지정가 주문**: 매도 지정가 ≤ 현재가 or 매수 지정가 ≥ 현재가 일 시 현재가로 즉시 체결
- **시장가 주문**: 장 운영 중일 시 현재가 즉시 체결
- 체결 결과는 Kafka `execution.event` 토픽으로 Spring Boot에 발행

### KIS WebSocket 클라이언트

한국투자증권 실시간 WebSocket에 연결하여 구독 종목의 체결가·호가를 수신합니다.

- 연결 끊김 시 자동 재연결 (Exponential Backoff)
- KIS 접속 토큰은 만료 전 자동 갱신 스케줄러 운영

### 실시간 시세 캐시 전략

| 데이터         | TTL    | 저장소                      |
| -------------- | ------ | --------------------------- |
| 현재가         | 24시간 | Redis `market:price:{code}` |
| 종목 상세 정보 | 7일    | Redis `market:info:{code}`  |
| 분봉(OHLCV)    | 7일    | Redis (Sorted Set)          |

### WebSocket Hub

- 클라이언트별 구독 심볼 관리 (`clientMeta.subscribedSymbols`)
- Spring Session 기반 userId 조회로 인증된 사용자에게만 개인 알림 push
- `BroadcastToAll()` / `BroadcastToUser()` 분리

### Rate Limiting

Redis Sliding Window 방식으로 전체 IP에 대해 **70req/s** 제한 적용

<br/>

## 🔄 Spring Boot와의 연동 포인트

| 연동 방식      | 방향        | 내용                                                                              |
| -------------- | ----------- | --------------------------------------------------------------------------------- |
| Kafka Consumer | Spring → Go | 주문 이벤트 수신 (`order.created` 토픽)                                           |
| Kafka Producer | Go → Spring | 체결 이벤트 발행 (`execution.event` 토픽)                                         |
| Redis 공유     | 양방향      | 시세 캐시(`market:price:*`), 계좌 잔고(`account:*`), 세션(`barleyssal:session:*`) |
| WebSocket      | Go → React  | 실시간 시세·체결·PnL 클라이언트 push                                              |
