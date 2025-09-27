# Каноничный сервис (Golden Record / MDM)

**Версия:** 1.0  
**Назначение документа:** полная техническая спецификация учебно-практического проекта уровня Middle+, отражающая архитектуру, сценарии, API, схемы данных, инфраструктуру, нефункциональные требования и план реализации.

---

## 1. Обзор

**Цель системы.** Собрать разнородные карточки объектов (далее — *items*, начнём с домена «товары») из нескольких источников, обнаружить дубликаты, объединить сведения в единую **каноническую запись (golden record)** с версионированием и распространять события об изменениях подписчикам.

**Ключевые свойства:** высокая доступность, горизонтальное масштабирование, наблюдаемость, отказоустойчивость, строгая управляемость транзакциями, реактивные чтения, надёжные интеграции и демонстрация промышленных практик.

**Целевые технологии (по делу):** Java 21, Spring Boot 3.x, WebFlux, Data JPA/R2DBC, Hibernate, Kafka (EOSv2), Postgres, Redis/Apache Ignite, Caffeine, Resilience4j, Micrometer→Prometheus, OpenTelemetry→Jaeger, Loki/ELK, Keycloak (OIDC), Testcontainers, Pact, JMH, Gatling/JMeter, Docker Compose.

---

## 2. Термины и глоссарий
- **Raw Item** — сырая карточка из источника (как прислали).  
- **Normalized Item** — нормализованная карточка (очистка/приведение).  
- **Similarity Graph** — граф похожести записей (рёбра — метрики близости).  
- **Cluster** — компонент связности/«подозреваемая группа дубликатов».  
- **Canonical Item** — каноническая запись, итог слияния кластера.  
- **Outbox** — таблица событий для транзакционной публикации в Kafka.  
- **Read Model** — материализованная проекция для быстрых чтений (CQRS).  

---

## 3. Сценарии высокого уровня
1. **Загрузка данных**: внешний источник POST’ит batch товаров в Ingestion → запись в Postgres в одной транзакции с Outbox → публикация события `raw.item.created` в Kafka.  
2. **Канонизация**: Canonicalizer читает `raw.items`, нормализует, обновляет граф похожести, выполняет кластеризацию (ForkJoin) и строит/обновляет `canonical.item` (+версия) → публикует `canonical.item.upserted`.  
3. **Чтение каталога**: Catalog Read подписывается на `canonical.item.*`, поддерживает read-модель (R2DBC) и отдаёт реактивные поисковые запросы.  
4. **Нотификации**: Notification Service слушает `canonical.item.*`, доставляет webhooks/email/SSE, применяет retry/CB/bulkhead, ведёт DLT.  
5. **Наблюдаемость**: все сервисы экспортируют системные и бизнес-метрики, логи c корреляцией traceId, трассировки; доступные дашборды/алерты.  
6. **Безопасность**: Keycloak выдаёт JWT, Gateway валидирует, сервисы применяют ограничение по ролям/скоупам.  

---

## 4. Нефункциональные требования (NFR)
- **Доступность:** 99.9% для чтений read-модели; деградация без падения (read-only режим при проблемах write-цепочки).  
- **Производительность:** P95 latency чтений ≤ 50 мс при 1k RPS; ingest batch 10k записей < 60 c.  
- **Масштабирование:** горизонтальное по сервисам и по Kafka consumer groups/партициям.  
- **Надёжность данных:** транзакционный outbox, EOSv2, идемпотентные обработчики.  
- **Наблюдаемость:** все ключевые пути покрыты метриками/трассировками; SLO и алерты.  
- **Безопасность:** OIDC, минимально необходимые роли, secrets в переменных окружения/SecretStore.  

---

## 5. Архитектура

### 5.1 Контекстная диаграмма
- **Клиенты/Интеграции** → API Gateway → { Ingestion | Catalog Read }  
- **Ingestion** ↔ Postgres(write) ↔ Outbox → Kafka(`raw.items`)  
- **Canonicalizer** ← Kafka(`raw.items`) ↔ Postgres(write) ↔ Cache(Redis/Ignite/Caffeine) → Kafka(`canonical.items`)  
- **Catalog Read** ← Kafka(`canonical.items`) ↔ Postgres(read) (R2DBC)  
- **Notifications** ← Kafka(`canonical.items`) → внешние Webhook/Email  
- **Наблюдаемость**: Micrometer→Prometheus, OTel→Collector→Jaeger, Логи→Loki

### 5.2 Состав сервисов

#### API Gateway (Spring Cloud Gateway, WebFlux)
**Ответственность:** единая точка входа, TLS, JWT верификация (Keycloak), rate limiting, routing, circuit breaker, propag. trace headers.  
**Взаимодействия:** проксирование `/api/ingestion/**`, `/api/catalog/**`.  
**Resilience:** Resilience4j filters (CB, retry, rate limiter).  
**Метрики:** http.server.*, gateway.*; бизнес-теги (clientId).  

#### Ingestion Service (Boot + JPA/Hibernate)
**Назначение:** приём сырых карточек; валидация; запись в Postgres; публикация событий через Outbox.  
**Транзакции:** `@Transactional` (REQUIRED); для Outbox — блок `REQUIRES_NEW` (вариант) или один коммит с триггером-сканером.  
**Hibernate/DB:** демонстрация LAZY/EAGER, проблема N+1 и решения (`@EntityGraph`, `join fetch`, `@BatchSize`), индексы (BTREE/GIN), блокировки `FOR UPDATE SKIP LOCKED` для конкурентных паблишеров.  
**Метрики:** `ingestion.requests.count`, `outbox.pending`, `outbox.publish.latency` (histogram).

#### Canonicalizer Service (Boot, многопоточность, Kafka)
**Назначение:** нормализация, построение/обновление графа похожести, кластеризация, формирование/обновление канонической записи, публикация событий.  
**Алгоритм:**  
- Предобработка: токенизация/нормализация, кэширование фич (Caffeine).  
- Граф: вершины — normalized items, рёбра — похожесть (Jaccard/Levenshtein/TF-IDF, настраиваемо Strategy).  
- Кластеризация: компоненты связности/threshold; параллельность — ForkJoinPool (RecursiveTask) + сравнение с parallel streams/virtual threads.  
- Слияние: правила приоритета источников (Strategy + Template Method), конфликтные поля — State/компенсации.  
**Кэш:** Caffeine (локальный, near cache) + Redis/Ignite (распределённый кэш фич/кластеров).  
**Kafka:** потребление `raw.items` (consumer group="canonicalizer"), публикация `canonical.item.upserted`. Exactly-Once: транзакционный producer, `isolation.level=read_committed`, согласованный commit offset’ов.  
**Метрики:** `similarity.compute.time` (distribution), `clusters.size`, `canonical.upserts.count`, `kafka.lag`.

#### Catalog Read Service (Boot + WebFlux + R2DBC)
**Назначение:** поддерживает read-модель канонических записей и отдаёт реактивное API для поиска/чтения.  
**Read-модель:** денормализованная таблица с основными полями и индексацией под поиск (GIN по jsonb, триграммы opclass — при необходимости).  
**Метрики:** p95 latency на `/search`, cache hit/miss при использовании локального кэша.  

#### Notification Service (Boot + Kafka + Resilience4j)
**Назначение:** доставка событий во внешние системы (webhook/email).  
**Надёжность:** retry с экспоненциальной задержкой и jitter, bulkhead для изоляции, circuit breaker для проблемных адресатов; DLT для неисправимых случаев.  
**Метрики:** `notify.success.rate`, `notify.retry.count`, `notify.dlt.count`.

#### Identity (Keycloak)
Realm `canonical`, клиенты `gateway`, `ingestion`, `catalog`, роли `INGESTOR`, `READER`, `ADMIN`.  
JWT содержит `scope/roles`, `clientId`, `exp`, `sub`.

---

## 6. Модель данных и схемы БД (Postgres)

### 6.1 Write-хранилище (Ingestion/Canonicalizer)
**Таблицы:**
- `source` (id PK, name UNIQUE, metadata jsonb).  
- `raw_item` (id PK, source_id FK, external_id, payload jsonb, created_at).  
  - Индексы: (source_id, external_id) UNIQUE; GIN(payload).  
- `normalized_item` (id PK, raw_item_id FK UNIQUE, norm jsonb, features jsonb, version int, created_at).  
  - Индексы: GIN(norm), GIN(features).  
- `cluster` (id PK, cluster_key text UNIQUE, stats jsonb, updated_at).  
- `cluster_member` (cluster_id FK, normalized_item_id FK, PRIMARY KEY(cluster_id, normalized_item_id)).  
- `canonical_item` (id PK, cluster_id FK UNIQUE, canonical jsonb, version int, updated_at).  
- `canonical_item_version` (id PK, canonical_item_id FK, version int, canonical jsonb, created_at).  
- `outbox_event` (id PK, aggregate_type, aggregate_id, type, payload jsonb, headers jsonb, created_at, published_at NULLABLE).  
  - Индексы: (published_at NULLS FIRST), BTREE по created_at; `SKIP LOCKED` для паблишера.  

**Версионирование:** `canonical_item.version` увеличивается при каждом изменении; полная копия — в `canonical_item_version`.

**Блокировки:** оптимистическая (`@Version` в JPA) на `normalized_item`/`canonical_item`; пессимистическая при конкурентной сборке кластера (внутренний «lock stripe» или advisory locks PG при необходимости).

### 6.2 Read-хранилище (Catalog Read, R2DBC)
- `catalog_item` (id PK, canonical_id, title, brand, attributes jsonb, searchable tsvector, updated_at).  
  - Индексы: BTREE(canonical_id), GIN(attributes), GIN(searchable).  
- Возможна материализованная вьюха для сложных поисков (REFRESH по событиям).

---

## 7. Событийная модель (Kafka)

### 7.1 Топики
- `raw.items` (Key: `sourceId#externalId`; Value: Avro `RawItemCreated`).  
- `canonical.items` (Key: `canonicalId`; Value: Avro `CanonicalItemUpserted`).  
- `notifications.dlt` (Key: destination; Value: failed event with error cause).  

### 7.2 Схемы сообщений (Avro/JSON-Schema)
- `RawItemCreated { rawItemId, sourceId, externalId, payload, createdAt }`  
- `CanonicalItemUpserted { canonicalId, clusterId, version, canonicalPayload, changeSummary, updatedAt }`  

### 7.3 Политики
- Партиционирование: по ключу; количество партиций масштабирует обработку.  
- Retention: `raw.items` — 7 дней; `canonical.items` — compacted + 7 дней (для истории).  
- Exactly-Once: транзакционный producer/consumer, уникальные `transactional.id` на инстанс.

---

## 8. API (через Gateway)

### 8.1 Ingestion API
- `POST /api/ingestion/v1/items` — загрузка сырых карточек (batch).  
  - Auth: `INGESTOR`.  
  - Ответ: `{ accepted, rejected[], traceId }`.  
- `GET /api/ingestion/v1/items/{id}` — получить сырую/нормализованную карточку.  

### 8.2 Catalog Read API (реактивный)
- `GET /api/catalog/v1/items/{canonicalId}` — получить каноническую запись.  
- `GET /api/catalog/v1/search?q=&brand=&limit=&offset=` — полнотекст/фильтры.  
- SSE (опционально): `/api/catalog/v1/stream` — стрим обновлений.  
  - Auth: `READER`.

### 8.3 Admin/Support API
- `GET /api/admin/v1/health` — агрегация healthchecks.  
- `GET /api/admin/v1/metrics` — Prometheus endpoint (скрыто за auth/restrict).  

**Стандарты:** RFC‑7807 для ошибок; корреляция по заголовкам `traceparent`, `X-Request-Id`.

---

## 9. Транзакции, Propagation, Outbox
- Базовые операции Ingestion — `@Transactional(REQUIRED)`; внутри — сохранение RawItem + запись Outbox.  
- Отдельный паблишер (Spring `SmartLifecycle`) выбирает пачки `outbox_event` с `published_at IS NULL`, помечает `FOR UPDATE SKIP LOCKED`, публикует в Kafka, атомарно выставляет `published_at`.  
- Альтернатива: `@Transactional(REQUIRES_NEW)` для записи Outbox при побочном действии (trade-off — лишний коммит).  
- Чтение из Kafka — идемпотентные обработчики (ключи дедупликации по aggregateId+version).  

---

## 10. Реактивность vs Блокирующие операции
- **Ingestion** — JPA (блокирующий I/O) оправдан строгими инвариантами и ACID.  
- **Catalog Read** — WebFlux + R2DBC ради масштабирования чтений; бэкап потока — `onBackpressureBuffer`/`limitRate`.  
- Изоляция контуров: write-сервисы блокирующие; read-сервисы — реактивные.  

---

## 11. Многопоточность и асинхронность
- Пулы:  
  - `canonicalizer-fjp` (ForkJoinPool) — CPU-bound кластеризация.  
  - `reactor-boundedElastic`/`virtual-threads` — интеграции/файловые операции.  
- Синхронизация: `StampedLock` для локальных структур; избегаем contention, используем `LongAdder` для счётчиков.  
- Паттерны: `CompletableFuture` для fan-out/fan-in, но предпочтение — реактивный пайплайн или FJP для CPU-задач.  

---

## 12. Кэширование
- **Caffeine**: near-cache на сервисах чтения и в Canonicalizer для фич похожести (TTL 5–15 мин, size-based eviction, метрики hit/miss).  
- **Redis/Ignite**: распределённый кэш кластеров/канонических записей (write-through под канонизацию, cache-aside под чтения; инвалидация по событиям `canonical.item.upserted`).  

---

## 13. Безопасность (AuthN/AuthZ)
- Keycloak Realm `canonical`: клиенты `gateway` (public), `ingestion`/`catalog` (confidential).  
- Роли: `INGESTOR`, `READER`, `ADMIN`; маппинг в `realm_access.roles`.  
- Сервисы как Resource Server: проверка подписи, аудит.  
- Методный уровень: `@PreAuthorize("hasRole('ADMIN')")` и предметные правила (например, доступ к источнику данных только его владельцу).  

---

## 14. Наблюдаемость и эксплуатация

### 14.1 Метрики (Micrometer → Prometheus)
**Системные:** `jvm.*`, `http.server.*`, `kafka.consumer.*`, `process.*`.  
**Технические:** `outbox.pending`, `outbox.publish.latency{source}`, `kafka.lag{topic,group}`.  
**Бизнес:** `canonical.upserts.count{source}`, `clusters.size.histogram`, `dedup.merge.ratio`, `read.search.latency`.  
**Сложные варианты:** histograms с SLA-бакетами, exemplars с traceId, high-cardinality контроль (не более 3–4 label’ов).

### 14.2 Логирование
- Logback JSON encoder; обязательные поля: `timestamp, level, logger, message, traceId, spanId, service, thread, http.requestId`.  
- Централизация: Promtail → Loki; ротация контейнерных логов.

### 14.3 Трейсинг (OpenTelemetry)
- Автоинструментирование Spring/Kafka/HTTP; `traceparent` сквозь Gateway.  
- Jaeger/Tempo для просмотра; связывание метрик ↔ трейсов через exemplars.

### 14.4 Дашборды и алерты
- Grafana:  
  - «Сводка сервиса» (CPU, Heap, GC, RPS, p95, error rate).  
  - «Kafka Consumer Lag» (по группам).  
  - «Бизнес» (merge ratio, clusters size).  
- Alertmanager правила: SLA breach (p95>threshold), consumer lag > N минут, error rate > X%.

---

## 15. Отказоустойчивость и устойчивость
- Resilience4j:  
  - **CircuitBreaker** для внешних webhook’ов;  
  - **Retry** с экспоненциальной задержкой и jitter;  
  - **Bulkhead** (semaphore/thread) для изоляции подсистем;  
  - **RateLimiter** для noisy neighbors.  
- Идемпотентность: ключи событий, дедупликация на уровне БД/кэша.  
- DLT: `notifications.dlt` + ручные/авто-реплеи.

---

## 16. Паттерны проектирования
- **DDD**: Aggregates — `Cluster`, `CanonicalItem`.  
- **CQRS**: write/read разделены.  
- **Outbox**: транзакционная публикация.  
- **Saga**: цепочки нотификаций с компенсирующими действиями.  
- **Strategy**: правила нормализации и слияния.  
- **Template Method**: последовательность шагов канонизации.  
- **State**: жизненный цикл кластера (draft → stable).  
- **Decorator**: оборачивание клиентов метриками/логами.  
- **Factory/AbstractFactory**: провайдеры нормализаторов/метрик похожести.

---

## 17. Исключения и обработка ошибок
- Иерархия: `DomainException` ← `CanonicalizationException`, `DuplicateGroupConflictException`, `ValidationException`, `OutboxPublishException`, …  
- REST-ошибки: RFC‑7807 (Problem JSON) с `type`, `title`, `detail`, `traceId`.  
- Стратегии: не маскировать системные сбои, добавлять корректный HTTP-код, ретраи только там, где идемпотентно.

---

## 18. Сериализация/десериализация
- REST: Jackson (настройки `WRITE_DATES_AS_TIMESTAMPS=false`, `JavaTimeModule`).  
- Kafka: Avro/JSON Schema, эволюция схем (backward compatible), кастомные сериализаторы для `BigDecimal/LocalDateTime`.  
- Хранение: Postgres jsonb для гибкости, но ключевые поля — в колонках для индексов.

---

## 19. Коллекции, итераторы, очереди (демо-задачи)
- **algorithms-модуль:**  
  - Реализация **BST** и **RB-Tree** (сравнение с `TreeMap`),  
  - Кастомные **итераторы** (fail-fast/fail-safe),  
  - Очереди: `ArrayDeque`, `LinkedBlockingQueue`, `PriorityQueue` (отложенные ретраи).  
  - Демонстрации: коллизии хэш-таблиц и переход в деревья (как в JDK8+).  
  - JMH-бенчмарки и отчёты сравнений.

---

## 20. Generics и Stream API
- **Generics:** `TypedId<T>`, обобщённые репозитории, `Result<T,E>`/`Either<L,R>`, типобезопасные ключи Kafka.  
- **Stream API:** нормализация/агрегации в Canonicalizer; критерии выбора против Reactor (IO-bound vs CPU-bound).

---

## 21. Скоупы бинов и жизненный цикл
- Скоупы: `singleton` (по умолчанию), `prototype` (одноразовые стейтфул-обработчики), `request` (в WebFlux), кастомный scope для batch-job (lifecycle-бин).  
- Инициализация: `@PostConstruct`, `InitializingBean`; consumers — через `SmartLifecycle` (управляемый старт/стоп, graceful shutdown).  

---

## 22. Развёртывание и окружения
- **Docker Compose**: `postgres`, `kafka` (+schema-registry + ui), `keycloak`, `prometheus`, `grafana`, `loki`+`promtail`, `otel-collector`, сервисы приложения.  
- Healthchecks и readiness-пробы; зависимость порядка старта (например, ожидать Kafka).  
- Конфигурация через `application.yml` + переменные окружения; секреты — через `.env`/Docker secrets.  

---

## 23. CI/CD и качество
- GitHub Actions: билд, тесты (unit+integration), сборка контейнеров, публикация образов.  
- Testcontainers: интеграции с Postgres/Kafka/Keycloak.  
- SonarQube/SpotBugs/Checkstyle (качество).  
- SBOM (syft/grype), уязвимости (Trivy).  

---

## 24. Тестирование
- **Unit** — бизнес-логика, алгоритмы похожести.  
- **Integration** — репозитории JPA/R2DBC, транзакции, outbox, Kafka поток.  
- **Contract (Pact)** — между Gateway↔Ingestion↔Catalog.  
- **Performance** — Gatling/JMeter (чтения), JMH (критичные структуры/алгоритмы).  
- **Chaos/Resilience (опц.)** — fault injection на уровень сетевых ошибок.

---

## 25. Эксплуатационные процедуры (Runbook)
- **Ингест встал:** проверить `outbox.pending` и логи паблишера; возможно, блокировки — запустить «разгрузочный» джоб.  
- **Рост `kafka lag`:** увеличить consumer-инстансы/партиции; проверить паузы GC/блокировки БД.  
- **Ошибки нотификаций:** смотреть CircuitBreaker state, объём DLT; реплей после устранения причины.  
- **Деградация чтений:** проверить индексы, план запросов, включить кэш на повышенный TTL.  
- **Бэкапы:** ежедневный dump write/read БД; восстановление — документированная процедура.  

---

## 26. Пошаговый план реализации (Milestones)

**M1 (Недели 1–2): Каркас и базовый поток**  
- Мономодуль → многомодуль (Gradle).  
- Compose-окружение (Postgres, Kafka(+UI), Keycloak, Prometheus, Grafana, Loki, OTel).  
- Ingestion: POST → JPA → Outbox → Kafka(`raw.items`).  
- Catalog Read: consumer `canonical.items` (заглушка) + базовый `/search`.  
- Метрики: системные + 2–3 кастомные.  
**Критерии приёмки:** сквозной POST→Kafka; дашборд видит RPS, p95.

**M2 (Недели 3–4): Транзакции, N+1, Resilience**  
- Полный Outbox-паблишер с `SKIP LOCKED`.  
- Демонстрация N+1 и три способа решения.  
- Gateway Resilience filters; Notification с retry/CB/bulkhead.  
- Exactly-Once producer/consumer.  
**Критерии:** нет дублирования событий; DLT на некорректных webhook’ах работает.

**M3 (Недели 5–6): Алгоритмы канонизации и кэш**  
- Similarity Graph, кластеризация (FJP), Strategy для метрик похожести.  
- Caffeine + Redis/Ignite; метрики hit/miss.  
- Публикация `canonical.item.upserted`; обновление read-модели.  
**Критерии:** на тестовом датасете сливаются дубликаты; p95 канонизации в допуске.

**M4 (Неделя 7): Наблюдаемость**  
- Доп. бизнес-метрики, histogram + exemplars; дашборды Grafana; трейсинг Jaeger end-to-end.  
**Критерии:** хотя бы 2 алерта; трейсы отображают путь POST→Kafka→Canonicalizer→Read.

**M5 (Неделя 8): Security**  
- Keycloak realm, роли/клиенты, проверка JWT на Gateway и сервисах, `@PreAuthorize`.  
**Критерии:** без токена/ролей доступ запрещён; аудиторские логи.

**M6 (Недели 9–10): Тесты и полировка**  
- Testcontainers покрывают Postgres/Kafka/Keycloak; Pact; JMH отчёты.  
- Документация README, схемы топиков, дампы дашбордов.  
**Критерии:** CI зелёный; reproducible deploy `docker compose up`.

---

## 27. Риски и компромиссы
- **Сложность алгоритмов**: нужно контролировать `O(n log n)`/`O(n^2)` шаги, профилировать на датасетах.  
- **Согласованность**: eventual consistency между write и read; SLA на задержку репликации.  
- **Кардинальность метрик**: осторожно с label’ами; иначе нагрузка на Prometheus.  
- **Эволюция схем**: строгая дисциплина совместимости Avro.

---

## 28. Дальнейшая дорожная карта (опционально)
- Admin UI (React) для ручной валидации слияний.  
- Feature flags (Togglz/Unleash).  
- Шардирование по доменам/источникам.  
- Специализированный поиск (OpenSearch) поверх read-модели.  
- Кросс-региональная репликация.

---

## 29. Приложения
- Пример доменных метрик похожести (конфиг).  
- Шаблон алертов Prometheus.  
- Конвенции именования топиков/метрик/логгеров.  
- Политика ретеншна для логов/трассировок.  

**Конец документа.**

