# Listener Hub Architecture

## Purpose

Bu dokuman, Eventra'nin onune eklenecek ayri bir `listener-hub` uygulamasinin mimarisini tarif eder.

Hedef:
- mobil, webhook, queue, harici Kafka topic veya baska kaynaklardan veri tuketmek
- veriyi Eventra event formatina cevirmek
- sonucu Kafka `event.raw` topic'ine publish etmek
- tum bu yonetimi veritabani olmadan, config dosyalari ile yapmak

Bu servis Eventra `rule-engine` yerine gecmez.  
`listener-hub`, Eventra icin bir entegrasyon ve normalizasyon katmani olur.

## Why Separate Application

Bu yapinin ayri uygulama olmasi gerekir, cunku:
- rule-engine domain mantigini korur
- entegrasyon karmasasi rule-engine'e tasinmaz
- 100-200 listener tanimi engine kodunu sisirmez
- dinleyici mantiklari bagimsiz deploy edilir
- log, retry, DLQ ve offset yonetimi ayri ele alinir

## Core Principle

Sistem DB-first degil, config-first calisir.

Yani:
- listener tanimlari config dosyalarindan gelir
- routing, mapping, retry ve filter kurallari config ile verilir
- uygulama bu config'leri yukler
- runtime'ta listener worker'larini olusturur

Ilk asamada zorunlu bir Oracle veya baska metadata DB yoktur.

## High-Level Flow

1. `listener-hub` config dosyalarini yukler
2. her listener tanimini validate eder
3. her listener icin uygun source adapter'i baslatir
4. gelen ham mesaj ortak event pipeline'a girer
5. filter uygulanir
6. mapping/transform uygulanir
7. Eventra event schema validation yapilir
8. gecerli event Kafka `event.raw` topic'ine publish edilir
9. hata olursa local log + DLQ / error queue mantigi devreye girer

## Proposed Service Name

Onerilen klasor:
- `services/listener-hub`

Onerilen sorumluluk:
- source listener runtime
- mapping/transform runtime
- publish runtime
- config reload
- metrics / health / last error visibility

## Supported Source Types

Ilk tasarimda source type kavrami olmalidir.

Olası source type'lar:
- `kafka_topic`
- `webhook`
- `http_poller`
- `oracle_query_poller`
- `file_drop`
- `redis_stream`
- `mqtt`

Ilk MVP icin en mantikli source type:
- `kafka_topic`

Cunku senin ihtiyacin:
- mobil taraftan baska topic / sistem event consume etmek
- bunu Eventra event'ine cevirmek

## Output Contract

Listener-hub her seyi Eventra'nin kabul ettigi ortak event formata cevirmelidir:

```json
{
  "event_id": "mobile-evt-123",
  "customer_id": "cust-42",
  "event_type": "mobile_app_open",
  "ts": "2026-04-04T12:00:00.000Z",
  "payload": {
    "screen": "home"
  },
  "source": "mobile-app"
}
```

Bu contract bozulmazsa Eventra API ve rule-engine degismeden calisir.

## Runtime Architecture

### 1. Config Loader

Gorev:
- config dosyalarini okumak
- schema validate etmek
- duplicate listener id kontrolu yapmak

Onerilen config konumlari:
- `config/listeners/*.json`
- `config/listeners/*.yaml`

Onerilen ana dosya:
- `config/listener-hub.json`

### 2. Source Adapter Layer

Her source type icin adapter olur.

Ornek:
- `adapters/kafka-topic.js`
- `adapters/webhook.js`
- `adapters/http-poller.js`

Adapter'in gorevi:
- kaynaga baglanmak
- ham mesaj almak
- ortak runtime'a vermek

Adapter Eventra event'i uretmek zorunda degildir.  
Sadece ham veriyi ve metadata'yi pipeline'a verir.

### 3. Filter Layer

Her listener icin opsiyonel filtre tanimlanir.

Ornekler:
- sadece belli event name'ler
- belli app version
- belli tenant
- belli country

Filtre sonucu:
- `accept`
- `drop`
- `route_to_dead_letter`

### 4. Mapping Layer

En kritik katmandir.

Ham input -> Eventra event donusumu burada yapilir.

Iki seviye desteklenmeli:

1. Declarative mapping
- basit path bazli mapping
- ornek:
```json
{
  "customer_id": "$.user.id",
  "event_type": "$.event.name",
  "ts": "$.event.timestamp",
  "payload.screen": "$.context.screen"
}
```

2. Scripted transform
- daha karmasik durumlar icin
- kucuk JS module veya inline transform

Ornek:
- event type'ini birden fazla alana gore secmek
- customer id fallback mantigi
- payload normalize etmek

### 5. Validation Layer

Mapping sonrasi uretilen Eventra event'i validate edilir.

Kontroller:
- `event_id` var mi
- `customer_id` var mi
- `event_type` var mi
- `ts` ISO date mi
- `payload` object mi
- `source` var mi

Validation gecmezse:
- event publish edilmez
- error log'a duser
- gerekirse DLQ'ya gider

### 6. Publisher Layer

Gorev:
- normalize edilen event'i Kafka `event.raw` topic'ine publish etmek

Onerilen davranis:
- `key = customer_id`
- `value = event JSON`

### 7. Error / Retry Layer

Her listener icin hata yonetimi ayri olmalidir.

Onerilen hata siniflari:
- source connect error
- source auth error
- parse error
- mapping error
- validation error
- publish error

Retry politikasi config ile verilir:
- `max_retries`
- `backoff_ms`
- `backoff_multiplier`

### 8. Observability Layer

Saglik endpoint'leri:
- `/health`
- `/listeners`
- `/listeners/:id/status`
- `/metrics`

Gorulmesi gereken durumlar:
- listener enabled mi
- son event ne zaman alindi
- son publish ne zaman oldu
- toplam success/fail sayisi
- son hata mesaji

## Config-First Design

## Main Rule

Listener davranisi koddan degil config'ten gelmelidir.

Kod sadece generic runtime olur.

Her listener icin ayri kod yazmak yerine:
- source type
- source config
- filter config
- mapping config
- retry config
- output config
tanimi verilir.

## Example Directory Layout

```text
services/listener-hub/
  src/
    index.js
    runtime/
      listener-manager.js
      pipeline.js
      publisher.js
    adapters/
      kafka-topic.js
      webhook.js
      http-poller.js
    transforms/
      mapping-engine.js
      script-runner.js
    schemas/
      listener-config.js
      event-schema.js
  config/
    listener-hub.json
    listeners/
      mobile-app-open.json
      mobile-purchase.json
      crm-user-update.json
```

## Example Global Config

```json
{
  "kafka": {
    "brokers": ["gbevmt02:9092"],
    "targetTopic": "event.raw"
  },
  "runtime": {
    "reloadOnChange": true,
    "defaultMaxRetries": 5
  }
}
```

## Example Listener Config

```json
{
  "listener_id": "mobile-app-events",
  "name": "Mobile App Events",
  "enabled": true,
  "source_type": "kafka_topic",
  "source": {
    "brokers": ["mobile-kafka-01:9092"],
    "topic": "mobile.raw.events",
    "group_id": "listener-hub-mobile-v1"
  },
  "filter": {
    "event_names": ["app_open", "purchase", "screen_view"]
  },
  "mapping": {
    "mode": "declarative",
    "fields": {
      "event_id": "$.meta.id",
      "customer_id": "$.user.customer_id",
      "event_type": "$.event.name",
      "ts": "$.event.ts",
      "source": "mobile-app",
      "payload.screen": "$.context.screen",
      "payload.platform": "$.device.platform"
    }
  },
  "output": {
    "topic": "event.raw",
    "key_field": "customer_id"
  },
  "retry": {
    "max_retries": 5,
    "backoff_ms": 1000
  }
}
```

## Mapping Modes

### Declarative Mode

Avantaj:
- hizli
- config ile yonetilir
- 100-200 listener icin olceklidir

Eksik:
- cok karmasik logic icin yetersiz olabilir

### Script Mode

Avantaj:
- esnek
- karmaşık kaynaklar icin uygun

Eksik:
- guvenlik ve kontrol yuzeyi artar
- debug zorlasir

Oneri:
- once declarative
- sadece zorunlu yerde script mode

## Listener Types Strategy

100-200 listener icin her listener'i ayri process yapma.

Daha dogru model:
- tek `listener-hub` process
- source type bazli adapter havuzu
- config'e gore birden cok listener runtime'ta acilir

Ornek:
- 80 `kafka_topic` listener
- 25 `webhook` listener
- 15 `http_poller` listener

Hepsi ayni generic runtime icinde yonetilebilir.

## Event Routing Strategy

Listener-hub tek bir ciktıya baglanabilir:
- `event.raw`

Alternatif:
- farkli ara topic'lere publish edip Eventra oncesi ayri router kullanmak

Ama mevcut Eventra ile en hizli entegrasyon icin:
- tum normalize event'ler `event.raw` topic'ine gitmeli

## State Management

Ilk versiyonda DB zorunlu olmamali.

State nerede tutulur:
- config dosyalari
- process memory
- gerekiyorsa local disk checkpoint dosyalari

Ornekler:
- webhook listener: state gerektirmez
- kafka listener: consumer group Kafka tarafinda offset tutar
- http poller: son watermark local state dosyasinda tutulabilir

Onerilen local state dizini:
- `/var/lib/eventra-listener-hub/`

## When Database May Be Added Later

Ilk surumde gerekmez.

Ileride DB ancak su ihtiyaclar icin dusunulebilir:
- merkezi listener yonetimi
- UI'dan listener tanimlama
- audit trail
- run history
- central offset/watermark state

Ama bu ikinci faz olur.  
Ilk tasarim config-first kalmalidir.

## Integration With Eventra

Baglanti noktasi:
- Eventra API ya da direkt Kafka

En dogru yol:
- listener-hub -> Kafka `event.raw`
- rule-engine -> mevcut haliyle tuketir

Neden:
- Eventra API yukunu arttirmaz
- event pipeline standard kalir
- ayni DLQ/consumer modeli korunur

Alternatif yol:
- listener-hub -> `POST /ingest`

Bu sadece dusuk hacimde mantikli olabilir.
Yuksek hacimde ana tercih olmamali.

## Security Model

- source credential'lari repo icinde tutulmaz
- env veya ayri secret file kullanilir
- script mode kullanilacaksa sandbox / whitelist dusunulmelidir
- outbound publish sadece izinli Kafka topic'lerine acik olmalidir

## MVP Proposal

Ilk MVP:
- `services/listener-hub`
- source type: `kafka_topic`
- mapping mode: `declarative`
- output: `event.raw`
- health/status endpoint'leri
- config reload
- log + retry

MVP sonrasi:
- `webhook`
- `http_poller`
- scripted transform
- local state checkpoint
- error topic / dedicated DLQ

## Suggested Implementation Phases

### Phase 1
- klasor ve runtime iskeleti
- config schema
- Kafka source adapter
- Kafka publisher
- declarative mapping
- health endpoint

### Phase 2
- retry / error policy
- local state file
- config hot reload
- metrics endpoint

### Phase 3
- webhook listener
- http poller
- scripted transform
- operator UI dusunulmesi

## Recommended Decision

Bu ihtiyac icin dogru teknik karar:
- ayri bir `listener-hub` uygulamasi
- Eventra'dan ayri deploy
- config-first yonetim
- DB zorunlu olmadan calisma
- Kafka `event.raw` ile Eventra'ya entegrasyon

Bu yaklasim:
- 100-200 listener senaryosunda olceklenir
- farkli mantiklari tek runtime'ta toplar
- Eventra engine'i sade tutar
- ileride UI veya DB ekleme opsiyonunu acik birakir
