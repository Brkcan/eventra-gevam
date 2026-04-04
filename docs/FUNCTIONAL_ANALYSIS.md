# Eventra Functional Analysis

## Purpose

Eventra, olay tabanli customer journey orkestrasyonu yapan bir uygulamadir. Sistem; event toplar, Oracle veritabanina yazar, tanimli journey akislarini calistirir, aksiyonlari tetikler, bekleyen instance'lari izler, cache dataset'leri yukler ve operator'e web arayuzu sunar.

Temel kullanim amaci:
- gelen customer event'lerini toplamak
- event'leri journey kurgularina baglamak
- wait / condition / action mantigi ile akis calistirmak
- manuel onay, manuel release ve yayin kontrolu saglamak
- operasyon ve gozlemleme ekranlari sunmak
- AI destekli journey taslagi, aciklama ve revizyon yardimi vermek

## Main Components

### Frontend

Konum:
- [apps/frontend/src/main.jsx](/Users/burakcan/Eventra-podman/apps/frontend/src/main.jsx)

Gorev:
- React Flow tabanli journey designer arayuzu
- journey listesi, folder yapisi ve version islemleri
- approval, publish, rollback ve manual wait operasyonlari
- dashboard ve catalogue ekranlari
- AI Copilot drawer

Kullanicinin gordugu ana alanlar:
- `Scenarios`
- `Catalogues`
- `Management`
- `Dashboards`

### API

Konum:
- [apps/api/src/index.js](/Users/burakcan/Eventra-podman/apps/api/src/index.js)

Gorev:
- frontend icin ana backend katmani
- event kabul etme ve Kafka'ya yayinlama
- Oracle CRUD islemleri
- journey kaydetme / yayinlama / approval
- customer profile, catalog, dashboard, logs ve manual wait endpoint'leri
- AI Copilot endpoint'leri

### Rule Engine

Konum:
- [services/rule-engine/src/index.js](/Users/burakcan/Eventra-podman/services/rule-engine/src/index.js)

Gorev:
- Kafka'dan `event.raw` topic'ini dinlemek
- publish edilmis journey'leri runtime bellekte tutmak
- event -> journey eslesmesi yapmak
- waiting / waiting_manual / processing / completed durumlarini yonetmek
- action tetiklemek
- transition ve action log yazmak

### Cache Loader

Konum:
- [apps/cache-loader/src/index.js](/Users/burakcan/Eventra-podman/apps/cache-loader/src/index.js)

Gorev:
- dis veri kaynaklarindan `SELECT` sorgusu calistirmak
- sonucu Redis dataset cache olarak yazmak
- job scheduler gibi calismak
- connection ve job yonetim API'si sunmak

### Shared Database Layer

Konum:
- [lib/database-config.mjs](/Users/burakcan/Eventra-podman/lib/database-config.mjs)
- [lib/database-runtime.mjs](/Users/burakcan/Eventra-podman/lib/database-runtime.mjs)

Gorev:
- Oracle baglanti konfigunu cozmlemek
- Oracle driver uzerinden uygulama icin uyumlu query katmani saglamak
- bind, CLOB ve row normalization farklarini gizlemek

## External Dependencies

### Oracle

Rol:
- ana kalici veri katmani

Oracle icinde tutulan ana yapilar:
- `events`
- `journeys`
- `journey_instances`
- `journey_instance_transitions`
- `action_log`
- `external_call_log`
- `customer_profiles`
- `journey_approvals`
- `journey_release_controls`
- `runtime_controls`
- `catalogue_*`
- `cache_loader_*`

Schema script:
- [infra/unix/oracle-schema.sql](/Users/burakcan/Eventra-podman/infra/unix/oracle-schema.sql)

### Kafka

Rol:
- event bus

Ana topic'ler:
- `event.raw`
- `action.triggered`
- `event.dlq`

### Redis

Rol:
- customer son durum cache'i
- cache dataset storage
- cache update pub/sub

Ana key yapilari:
- `customer:{customer_id}`
- `cache:dataset:{dataset_key}`
- `cache:dataset:{dataset_key}:meta`

## Functional Domains

## 1. Event Ingestion

Baslangic noktasi:
- `POST /ingest`

Akis:
1. API event payload'i alir
2. event Oracle `events` tablosuna yazilir
3. duplicate ise `duplicate_ignored` doner
4. Redis customer state guncellenir
5. Kafka `event.raw` topic'ine mesaj gonderilir
6. rule-engine bu eventi tuketir

Beklenen ana alanlar:
- `customer_id`
- `event_type`
- `ts`
- `payload`
- `source`

## 2. Journey Design and Publishing

Baslangic noktasi:
- `POST /journeys`
- `GET /journeys`
- `POST /journeys/:journeyId/clone-version`
- `DELETE /journeys/:journeyId`

Journey modeli:
- `trigger`
- `wait`
- `cache_lookup`
- `http_call`
- `condition`
- `action`

Publish kurali:
- bir version `published` olacaksa approval gerekir
- approval akisi:
  - `POST /journeys/:journeyId/request-approval`
  - `POST /journeys/:journeyId/approve`
  - `POST /journeys/:journeyId/reject`

Yardimci endpoint:
- `GET /journeys/:journeyId/approval`
- `GET /journeys/:journeyId/diff`

## 3. Runtime Journey Execution

Rule-engine runtime mantigi:
1. publish durumundaki journey'leri Oracle'dan yukler
2. gelen event ile `trigger_event_type` eslestirir
3. uygun journey icin instance olusturur veya yeniler
4. wait node varsa `waiting` veya `waiting_manual` durumuna alir
5. zamani gelen veya manuel release edilen instance'lari `processing` durumuna alir
6. condition, cache lookup, http call ve action mantigini uygular
7. tamamlanmis instance'i `completed` yapar
8. action log ve transition log yazar

Ana state'ler:
- `waiting`
- `waiting_manual`
- `processing`
- `completed`

## 4. Manual Wait Management

Baslangic noktasi:
- `GET /manual-wait-queue`
- `POST /manual-wait-release`

Kullanim:
- operator manual wait durumundaki customer'lari listeler
- secilen journey ve wait node icin queue gorulur
- belli sayida instance manuel olarak serbest birakilir
- instance `waiting_manual` -> `waiting` olur
- rule-engine bunu alip sonrasinda action'a ilerletir

## 5. Customer Profiles

Baslangic noktasi:
- `POST /customers`
- `PUT /customers/:customerId/profile`
- `GET /customers/:customerId/profile`
- `GET /customers`

Amac:
- customer segment ve attribute verisini tutmak
- `segment_match` condition'larinda kullanmak

## 6. Catalog Management

Baslangic noktasi:
- `GET /catalogues/summary`
- `GET/POST/PUT/DELETE /catalogues/event-types`
- `GET/POST/PUT/DELETE /catalogues/segments`
- `GET/POST/PUT/DELETE /catalogues/templates`
- `GET/POST/PUT/DELETE /catalogues/endpoints`
- `GET /catalogues/cache-datasets`

Amac:
- journey designer'a kontrollu, katalog tabanli veri saglamak
- operator'un event type, template, endpoint ve segment tanimlarini yonetmesini saglamak

## 7. Dashboard and Observability

Baslangic noktasi:
- `GET /dashboard/kpi`
- `GET /dashboard/journey-performance`
- `GET /dashboard/cache-health`
- `GET /actions`
- `GET /external-calls`
- `GET /journey-instances`
- `GET /journey-instance-transitions`
- `GET /dlq-events`
- `GET /edge-capacity-usage`

Gosterilen ana metrikler:
- event sayisi
- aktif / waiting / processing instance sayisi
- tamamlanan journey sayisi
- action success / fail oranlari
- cache dataset sagligi
- top journey listeleri

## 8. Release Controls and Global Pause

Baslangic noktasi:
- `GET /management/global-pause`
- `PUT /management/global-pause`
- `GET /management/release-controls`
- `PUT /management/release-controls/:journeyId`

Amac:
- tum sistemi global durdurmak
- journey bazli rollout yuzdesi vermek
- journey release pause uygulamak

## 9. Cache Loader

Baslangic noktasi:
- `GET /connections`
- `POST /connections`
- `DELETE /connections/:id`
- `POST /connections/:id/test`
- `GET /jobs`
- `POST /jobs`
- `PUT /jobs/:id`
- `DELETE /jobs/:id`
- `POST /jobs/:id/run-now`
- `POST /jobs/test-query`
- `GET /runs`

Fonksiyon:
- operator bir connection tanimlar
- bir job yaratir
- job bir `SELECT` sorgusu ve `dataset_key` ile tanimlanir
- scheduler ya da manuel calistirma ile veri alinir
- Redis dataset cache guncellenir
- rule-engine cache lookup node'larinda bu dataset kullanilir

## 10. AI Copilot

Backend:
- `POST /ai/generate-journey`
- `POST /ai/explain-journey`
- `POST /ai/revise-journey`

Frontend:
- designer icindeki `AI Sor` drawer

Fonksiyonlar:
- prompt'tan draft journey olusturmak
- mevcut journey'yi aciklamak
- mevcut journey'yi revize etmek

Calisma modu:
- `mock`
- `openai`

Not:
- AI ciktilari draft / yardimci niteliktedir
- publish karari kullanicidadir

## Main User Flows

### Flow A: Journey Kur ve Yayina Al

1. Kullanici frontend'de journey tasarlar
2. Gerekirse AI Copilot ile draft olusturur veya revize eder
3. Journey kaydedilir
4. Approval talebi gonderilir
5. Onay verilir
6. Journey publish edilir
7. Rule-engine bir sonraki refresh'te journey'yi runtime'a alir

### Flow B: Event Isleme

1. Sistem `POST /ingest` ile event alir
2. API event'i Oracle'a yazar
3. API event'i Kafka'ya yollar
4. Rule-engine event'i tuketir
5. Uygun journey instance'i olusturur
6. Gerekirse wait / condition / action calisir
7. Sonuc log'lanir

### Flow C: Manual Wait Operasyonu

1. Kullanici `manual wait queue` ekranini acar
2. Belirli journey ve wait node icin bekleyen customer'lari gorur
3. `release` islemi yapar
4. Rule-engine instance'i islemeye devam eder
5. Action tetiklenir ve log yazilir

### Flow D: Cache Kullanimi

1. Operator cache-loader ile dataset job tanimlar
2. Job veri cekip Redis'e yazar
3. Journey icindeki `cache_lookup` node bu dataset'i kullanir
4. Cache miss / default / fail davranisi node ayarina gore ilerler

## User Roles

### Business User
- journey taslagi olusturur
- AI Copilot ile prompt bazli draft ister
- mevcut journey'yi gorsel olarak duzenler
- temel dashboard ekranlarini izler

### Operator
- manual wait queue yonetir
- cache-loader connection ve job tanimlar
- katalog verilerini gunceller
- runtime ve action log ekranlarini izler

### Reviewer / Approver
- approval talebini inceler
- journey version'larina `approve` veya `reject` verir
- publish oncesi kontrol noktasi olur

### System Administrator
- Oracle / Redis / Kafka baglantilarini saglar
- Unix sunucuda `systemd` servislerini yonetir
- `caddy` ve domain ayarlarini yapar
- secret, env ve log yonetimini saglar

## Integration Matrix

### Oracle
- tum kalici veri burada tutulur
- journey, event, instance, action, approval, dashboard ve catalogue verileri buradadir

### Kafka
- API'den rule-engine'e event aktarimi icin kullanilir
- `event.raw` ana topic'tir
- `action.triggered` ve `event.dlq` yardimci topic'lerdir

### Redis
- customer son olay ozetleri
- cache dataset storage
- cache update bildirimleri

### SMTP
- email action icin gerekir
- `EMAIL_DRY_RUN=true` ile test modunda tutulabilir

### External HTTP Endpoints
- `http_call` node'lari ile dis servis entegrasyonu yapilir
- sonuc `external_call_log` icine yazilir

### AI Provider
- `mock` veya `openai`
- AI Copilot backend tarafindan cagrilir

## Operational Ownership

### Frontend Ownership
- journey designer
- dashboard
- catalog ve approval UI
- AI drawer deneyimi

Ana dosya:
- [apps/frontend/src/main.jsx](/Users/burakcan/Eventra-podman/apps/frontend/src/main.jsx)

### API Ownership
- operator UI ve servisler icin REST surface
- journey persistence
- approval/publish guard
- dashboard veri kaynaklari
- AI endpoint orchestration

Ana dosya:
- [apps/api/src/index.js](/Users/burakcan/Eventra-podman/apps/api/src/index.js)

### Rule Engine Ownership
- event tuketimi
- runtime journey state machine
- wait/condition/action isleme
- transition ve action log

Ana dosya:
- [services/rule-engine/src/index.js](/Users/burakcan/Eventra-podman/services/rule-engine/src/index.js)

### Cache Loader Ownership
- dataset refresh scheduler
- dis veri kaynagi sorgulari
- Redis dataset yazimi

Ana dosya:
- [apps/cache-loader/src/index.js](/Users/burakcan/Eventra-podman/apps/cache-loader/src/index.js)

## Risks And Technical Debt

### Infrastructure Risks
- Oracle schema otomatik olusmaz; manuel script uygulanmasi gerekir
- Kafka ve Redis zorunlu harici bagimliliklardir
- Kafka `advertised.listeners` yanlis ise sistem event alamaz

### Product Risks
- AI Copilot draft'lari yanlis varsayim uretebilir
- publish sureci approval ile korunur ama insan hatasi yine mumkundur
- cache dataset gecikirse journey condition sonuclari etkilenebilir

### Technical Risks
- Oracle uyum katmani bind ve CLOB davranisina hassastir
- runtime state machine tek dosyada yogun mantik barindirir
- cache katmani Redis varsayimina baglidir; baska cache teknolojisine gecis ek is cikarir

### Operational Risks
- servisler ayrik oldugu icin env ve port uyumsuzluklari kolayca sistem davranisini bozabilir
- systemd log / restart politikasi dogru ayarlanmazsa teshis zorlasir

## Non-Functional Requirements

### Availability
- `api`, `rule-engine`, `cache-loader` ayrik servisler olarak ayaga kalkabilmelidir
- servisler `systemd` altinda otomatik restart alabilmelidir
- Oracle, Kafka veya Redis kisa sureli erisim problemlerinde servisler kontrollu sekilde retry etmelidir

### Performance
- `ingest` endpoint'i event kabulunu hizli yapmali, agir is mantigini Kafka sonrasina birakmalidir
- rule-engine event tuketimini near-real-time seviyesinde yapmalidir
- dashboard sorgulari operasyonel gozlemleme icin yeterince hizli donmelidir

### Scalability
- `rule-engine` ve `cache-loader` gerekirse ayri sunuculara tasinabilmelidir
- Kafka ayri sunucuda konumlanabilir
- Oracle merkezi kurumsal veritabani olarak kullanilabilir

### Maintainability
- servisler moduler ayri uygulamalar olarak gelistirilmeye devam edilmelidir
- deploy dokumani env merkezli olmalidir
- schema scriptleri ve smoke test dosyalari repo icinde bulunmalidir

### Observability
- servisler `journalctl` ile izlenebilmelidir
- action, transition, external call ve DLQ verileri operasyonel teshis saglamalidir
- saglik endpoint'leri tum ana servislerde bulunmalidir

## Security Considerations

### Secret Management
- Oracle, SMTP, OpenAI ve diger secret'lar repoya yazilmamalidir
- secret'lar `/etc/eventra/eventra.env` gibi host tabanli env dosyalarinda tutulmalidir
- ifsa olan anahtarlar rotate edilmelidir

### Access Control
- approval akisi publish oncesi kontrol kapisi olarak kalmalidir
- production ortaminda AI draft'lari dogrudan publish edilmemelidir
- catalogue ve management endpoint'leri yetkilendirme katmani ile korunmalidir

### Network Security
- Oracle, Redis ve Kafka gereksiz acik portlarla internet'e acilmamalidir
- servisler mumkunse private network veya loopback uzerinden haberlesmelidir
- Caddy yalnizca gerekli public endpoint'leri expose etmelidir

### Data Handling
- event payload ve customer attributes hassas veri icerebilir
- log'lara gereksiz PII yazilmamasi gerekir
- cache dataset'lerinde hassas veri tutuluyorsa TTL/retention politikasi dusunulmelidir

## Deployment Topology Examples

### Topology A: Single Unix Host
- `frontend`, `api`, `rule-engine`, `cache-loader`, `redis`, `kafka` ayni sunucudadir
- Oracle ayri kurumsal DB olabilir
- kucuk ve orta olcekli kurulumlar icin en kolay modeldir

### Topology B: Kafka Ayrik Host
- `gbevmt01`: frontend, api, rule-engine, cache-loader, redis
- `gbevmt02`: kafka
- env tarafinda:
  - `KAFKA_BROKERS=gbevmt02:9092`

### Topology C: Shared Corporate Oracle
- uygulama sunuculari Oracle kurmaz
- sadece sirket Oracle'ina baglanir
- env tarafinda:
  - `ORACLE_CONNECT_STRING=<host>:1521/<service>`

### Topology D: Split Runtime
- `api` ve `frontend` bir sunucuda
- `rule-engine` ve `cache-loader` ayri sunucuda
- ortak Oracle, Kafka ve Redis adresleri env ile verilir

## Main Data Objects

### Event
- dis sistemden gelen tekil olay
- customer ile iliskilidir
- `event_type` ve `payload` tasir

### Journey
- version'li akis tanimi
- `graph_json` icinde node ve edge tutar

### Journey Instance
- bir customer icin bir journey'nin runtime calisan kopyasi

### Action Log
- tetiklenen action sonuc kaydi

### Transition Log
- instance state degisim tarihi

### Catalogue Records
- event types
- segments
- templates
- endpoints

## Deployment Model

Local:
- Docker sadece Oracle + Redis + Kafka icin kullanilabilir
- uygulama servisleri lokal process olarak calisir

Sunucu:
- container'siz Unix kurulum hedeflenir
- `api`, `rule-engine`, `cache-loader` ayrica `systemd` servisi olur
- `frontend` static build olarak `caddy` ile servis edilir
- Oracle sirket ici ya da ayri sunucuda olabilir
- Kafka ve Redis ayni sunucuda veya ayri sunucuda konumlanabilir

## Current Constraints

- Oracle schema otomatik bootstrap edilmez; script manuel uygulanir
- Kafka ve Redis harici bagimliliklar olarak zorunludur
- AI `openai` modunda kota / key gerektirir; `mock` mod test icin uygundur
- cache katmani Redis varsayimi ile yazilmistir

## File References

- API: [apps/api/src/index.js](/Users/burakcan/Eventra-podman/apps/api/src/index.js)
- Rule engine: [services/rule-engine/src/index.js](/Users/burakcan/Eventra-podman/services/rule-engine/src/index.js)
- Cache loader: [apps/cache-loader/src/index.js](/Users/burakcan/Eventra-podman/apps/cache-loader/src/index.js)
- Frontend: [apps/frontend/src/main.jsx](/Users/burakcan/Eventra-podman/apps/frontend/src/main.jsx)
- Oracle schema: [infra/unix/oracle-schema.sql](/Users/burakcan/Eventra-podman/infra/unix/oracle-schema.sql)
- Unix deploy: [infra/unix/README.md](/Users/burakcan/Eventra-podman/infra/unix/README.md)
