# Eventra MVP Bootstrap

Bu repo, EVAM benzeri bir urun icin ilk MVP iskeletini kurar.

## Icerik

- Kafka event ingestion
- API (`apps/api`) ile event alma ve persist etme
- Basit rule engine (`services/rule-engine`) ile `cart_add -> email action` tetikleme
- Redis customer son durum cache'i
- Postgres kalici veri
- React Flow journey designer (`apps/frontend`)

## Baslatma

1. Altyapi:

```bash
docker compose up -d
```

2. Paketleri yukle:

```bash
npm install
```

3. Ortam degiskenleri:

```bash
cp .env.example .env
```

4. Servisleri ayri terminallerde calistir:

```bash
npm run dev:api
npm run dev:rule-engine
npm run dev:frontend
npm run dev:cache-loader
```

## Podman ile Local Full Stack

Bu repo, Podman ile tum servisleri container icinde local calistirmak icin `docker-compose.podman.yml` override dosyasi icerir.

1. Podman VM'i baslat (macOS):

```bash
podman machine init
podman machine start
```

`podman machine init` daha once yapildiysa tekrar gerekmez.

2. Podman compose provider'ini kontrol et:

```bash
podman compose version
```

3. Local env dosyasini hazirla:

```bash
cp .env.example .env
```

4. Tum stack'i ayağa kaldir:

```bash
npm run podman:up
```

5. Durumu kontrol et:

```bash
npm run podman:ps
podman compose -f docker-compose.yml -f docker-compose.podman.yml logs -f api
```

6. Saglik kontrolleri:

```bash
curl http://localhost:3001/health
curl http://localhost:3002/health
curl http://localhost:3010/health
```

Beklenen local adresler:
- Frontend: `http://localhost:3000`
- API: `http://localhost:3001`
- Rule engine health: `http://localhost:3002/health`
- Cache loader: `http://localhost:3010`

Durdurma ve temizlik:

```bash
npm run podman:down
```

Volume/network dahil temizlemek istersen:

```bash
podman compose -f docker-compose.yml -f docker-compose.podman.yml down -v
```

Kisa komutlar:

```bash
npm run podman:up
npm run podman:ps
npm run podman:logs
npm run podman:down
```

## Ornek Event Gonderimi

```bash
curl -X POST http://localhost:3001/ingest \
  -H 'content-type: application/json' \
  -d '{
    "customer_id":"cust-123",
    "event_type":"cart_add",
    "payload":{"product_id":"p-42","price":1499},
    "source":"web"
  }'
```

Bu event `event.raw` topic'ine gider. Rule engine kurala uyarsa `action_log` tablosuna kayit atar ve `action.triggered` topic'ine aksiyon yazar.

## Cache Loader (Ayrı Uygulama)

`apps/cache-loader` ayrı bir scheduler uygulamasıdır.

Amac:
- Dış DB'ye `SELECT` query çalıştırmak
- Sonucu Redis cache dataset olarak yazmak
- Günlük belirlenen saatte otomatik çalıştırmak (örn: `07:00`)

UI:
- `http://localhost:3010`

Akış:
1. Connection ekle (host/port/db/user/pass)
2. Job ekle:
   - `dataset_key`
   - `sql_query` (sadece SELECT)
   - `key_column`
   - `run_time` (`HH:mm`)
   - `timezone` (örn `Europe/Istanbul`)
3. `Run Now` ile manuel test et
4. Scheduler her gün belirtilen saatte otomatik çalıştırır

Redis yazımı:
- Hash: `cache:dataset:{dataset_key}`
- Meta: `cache:dataset:{dataset_key}:meta`
- Pub/Sub: `cache.updated`

## Journey API

Journey listesi:

```bash
curl http://localhost:3001/journeys
```

Journey publish/update:

```bash
curl -X POST http://localhost:3001/journeys \
  -H 'content-type: application/json' \
  -d '{
    "journey_id":"cart_abandonment_v1",
    "version":1,
    "name":"Cart Abandonment 30m",
    "status":"published",
    "graph_json":{"nodes":[{"id":"trigger","type":"trigger"},{"id":"wait","type":"wait"}]}
  }'
```

Journey instance listesi:

```bash
curl "http://localhost:3001/journey-instances?customer_id=cust-123&limit=10"
```

## Branching Condition (True/False Edge)

- Frontend'de condition node'dan cikan edge'i sec.
- Sag panelde `Condition Branch` alanini `true` veya `false` yap.
- Rule engine bu label/data bilgisini okuyup ilgili action path'ine gider.

## Customer Profile API (segment_match)

Profil set et:

```bash
curl -X PUT http://localhost:3001/customers/cust-123/profile \
  -H 'content-type: application/json' \
  -d '{"segment":"vip","attributes":{"tier":3}}'
```

Profil getir:

```bash
curl http://localhost:3001/customers/cust-123/profile
```

- Condition node'da `condition_key = segment_match` sec.
- `condition_segment_value` alanina segment adi yaz (ornek: `vip`).

## Email Delivery (SMTP)

`.env` icine SMTP bilgilerini ekle:

```bash
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_SECURE=false
SMTP_USER=your_user
SMTP_PASS=your_pass
SMTP_FROM="Eventra <no-reply@yourdomain.com>"
EMAIL_DRY_RUN=false
```

Customer profile'da email tut:

```bash
curl -X PUT http://localhost:3001/customers/cust-123/profile \
  -H 'content-type: application/json' \
  -d '{"segment":"vip","attributes":{"email":"customer@example.com"}}'
```

- Journey action node `channel=email` oldugunda worker SMTP ile gondermeyi dener.
- Sonuc `action_log.status` alanina `sent` veya `failed` olarak yazar.

## Production Deployment (Internet Acik)

Bu adimlar sunucuda (Ubuntu 22.04/24.04) uygulanir.

1. Sunucuya baglan:

```bash
ssh root@SUNUCU_IP
```

2. Docker kur:

```bash
curl -fsSL https://get.docker.com | sh
apt-get update && apt-get install -y docker-compose-plugin
```

3. Projeyi kopyala:

```bash
git clone <repo-url> Eventra
cd Eventra
```

4. Production env hazirla:

```bash
cp .env.prod.example .env.prod
nano .env.prod
```

Guncellenecek zorunlu alanlar:
- `APP_DOMAIN` (or: `app.senin-domain.com`)
- `API_DOMAIN` (or: `api.senin-domain.com`)
- `ACME_EMAIL`
- SMTP ayarlari

5. DNS ayari yap:
- `A` kaydi: `APP_DOMAIN` -> sunucu IP
- `A` kaydi: `API_DOMAIN` -> sunucu IP

6. Uygulamayi ayağa kaldir:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

7. Kontrol et:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f caddy
```

8. Saglik testi:

```bash
curl https://$API_DOMAIN/health
```

Beklenen:
- Frontend: `https://$APP_DOMAIN`
- API: `https://$API_DOMAIN`

### Notlar

- Prod'da sadece Caddy 80/443 portlarini aciyor.
- Postgres/Redis/Kafka disariya acik degil.
- API domain HTTPS oldugu icin frontend otomatik olarak HTTPS API'yi kullanir.

## Podman ile Production Deploy (Unix Sunucu)

Podman ile sunucuda deploy icin bu repo artik `docker-compose.podman.prod.yml` dosyasi icerir.

1. Podman kurulumu ve servis kontrolu:

```bash
podman --version
podman compose version
```

2. Projeyi sunucuya al:

```bash
git clone <repo-url> Eventra
cd Eventra
```

3. Production env dosyasini hazirla:

```bash
cp .env.prod.example .env.prod
nano .env.prod
```

Zorunlu alanlar:
- `APP_DOMAIN`
- `API_DOMAIN`
- `ACME_EMAIL`
- SMTP ayarlari

4. DNS kayitlarini sunucu IP'sine yonlendir.

5. Deploy et:

```bash
npm run podman:prod:up
```

6. Durumu kontrol et:

```bash
npm run podman:prod:ps
npm run podman:prod:logs
curl https://$API_DOMAIN/health
```

Kapatma:

```bash
npm run podman:prod:down
```

Sunucuda Podman rootless calisiyorsa `80/443` port bind islemi izin problemi yasatabilir.
Bu durumda ya rootful Podman kullanilir ya da host uzerinde dusuk port izni acilir.

## VPS icin Daha Stabil Secenek: Caddy Hostta, Uygulama Podman'da

Eger VPS uzerinde Podman container icindeki Caddy, ACME/Let's Encrypt DNS timeout yasiyorsa
`docker-compose.podman.prod.host-proxy.yml` dosyasini kullan.

Bu senaryoda:
- API hostta `127.0.0.1:3001`
- Frontend hostta `127.0.0.1:3000`
- Cache loader hostta `127.0.0.1:3010`
- Caddy container icinde degil, dogrudan VPS uzerinde calisir

Podman stack:

```bash
podman-compose -f docker-compose.podman.prod.host-proxy.yml up -d --build
```

Host Caddyfile ornegi:

```caddy
www.tekinspot.com {
    reverse_proxy 127.0.0.1:3000
}

api.tekinspot.com {
    reverse_proxy 127.0.0.1:3001
}
```

Bu model, VPS DNS cozumlemesini host isletim sistemi uzerinden yaptigi icin
container icindeki ACME DNS problemlerini by-pass eder.

## VPS icin En Guvenli Podman Secenegi: Host Loopback Baglantilari

Eger Podman service discovery (`postgres`, `redis`, `kafka`) VPS uzerinde
`EAI_AGAIN` benzeri DNS hatalari uretiyorsa `docker-compose.podman.prod.vps.yml`
dosyasini kullan.

Bu modelde:
- Postgres hostta `127.0.0.1:5432`
- Redis hostta `127.0.0.1:6379`
- Kafka hostta `127.0.0.1:9092`
- API host network ile `127.0.0.1:3001`
- Rule engine host network ile `127.0.0.1:3002`
- Cache loader host network ile `127.0.0.1:3010`
- Frontend `127.0.0.1:3000`
- Host Caddy bu loopback portlara reverse proxy yapar

Deploy:

```bash
podman-compose -f docker-compose.podman.prod.vps.yml up -d --build
```

Host Caddyfile:

```caddy
www.tekinspot.com {
    reverse_proxy 127.0.0.1:3000
}

api.tekinspot.com {
    reverse_proxy 127.0.0.1:3001
}
```
