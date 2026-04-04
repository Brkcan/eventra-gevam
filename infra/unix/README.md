# Unix Deployment

Bu dizin, Eventra'yi Docker/Podman kullanmadan klasik Unix sunucuda ayri servisler olarak calistirmak icin gereken iskeleti icerir.

Not:
- Bu dokuman sunucu/production icindir.
- Local test icin Docker kullanim iskeleti root README'de ve [infra/docker/docker-compose.local.yml](/Users/burakcan/Eventra-podman/infra/docker/docker-compose.local.yml) dosyasinda bulunur.

Oracle gecis notu:
- Hedef ana veritabani Oracle'dir.
- Env ve deploy dokumani Oracle merkezli hale getirildi.
- Ayrintili gecis plani: [ORACLE_MIGRATION.md](ORACLE_MIGRATION.md)
- Oracle tablo scriptleri: [oracle-schema.sql](oracle-schema.sql)
- Oracle smoke test scripti: [oracle-smoke-test.sql](oracle-smoke-test.sql)
- Oracle smoke wrapper: [run-oracle-smoke.sh](run-oracle-smoke.sh)

Hedef yapi:

- `api` -> `systemd` servisi
- `rule-engine` -> `systemd` servisi
- `cache-loader` -> `systemd` servisi
- `frontend` -> `vite build` sonucu static dosya, `caddy` ile servis edilir
- `oracle`, `redis`, `kafka` -> host servisleri

## Sunucuda Ilk Kurulum

Asagidaki sirayi aynen uygularsan Unix sunucuda ilk kurulum tamamlanir.

### 1. Gereken paketleri kur

```bash
apt-get update
apt-get install -y curl git build-essential caddy redis-server
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs
```

Not:
- Oracle bu makinede host servis olmak zorunda degil; sirket Oracle'ina agdan baglanabilirsin.
- Redis ve Kafka bu sunucuda yerel servis olarak olmalidir.

### 2. Kodu yerlestir

```bash
mkdir -p /opt/eventra
git clone <repo-url> /opt/eventra/current
cd /opt/eventra/current
npm ci
```

### 3. Ortam dosyasini yaz

```bash
mkdir -p /etc/eventra
cp /opt/eventra/current/.env.example /etc/eventra/eventra.env
```

En az su alanlari duzenle:

```bash
KAFKA_BROKERS=127.0.0.1:9092
DB_VENDOR=oracle
ORACLE_USER=eventra
ORACLE_PASSWORD=secret
ORACLE_CONNECT_STRING=10.0.0.15:1521/ORCLPDB1
REDIS_URL=redis://127.0.0.1:6379
PORT=3001
RULE_ENGINE_HEALTH_PORT=3002
CACHE_LOADER_PORT=3010
CACHE_LOADER_METADATA_DB_VENDOR=oracle
CACHE_LOADER_METADATA_ORACLE_USER=eventra
CACHE_LOADER_METADATA_ORACLE_PASSWORD=secret
CACHE_LOADER_METADATA_ORACLE_CONNECT_STRING=10.0.0.15:1521/ORCLPDB1
LLM_PROVIDER=mock
```

Frontend build icin:

```bash
APP_DOMAIN=www.example.com
API_DOMAIN=api.example.com
ACME_EMAIL=ops@example.com
```

### 4. Oracle schema'yi uygula

```bash
cd /opt/eventra/current
sqlplus eventra/secret@10.0.0.15:1521/ORCLPDB1 @infra/unix/oracle-schema.sql
ORACLE_USER=eventra ORACLE_PASSWORD=secret ORACLE_CONNECT_STRING=10.0.0.15:1521/ORCLPDB1 ./infra/unix/run-oracle-smoke.sh
```

### 5. Frontend build al

```bash
cd /opt/eventra/current
VITE_API_BASE_URL=https://api.example.com npm --workspace @eventra/frontend run build
```

### 6. systemd servislerini yukle

```bash
cp /opt/eventra/current/infra/unix/systemd/eventra-api.service /etc/systemd/system/
cp /opt/eventra/current/infra/unix/systemd/eventra-rule-engine.service /etc/systemd/system/
cp /opt/eventra/current/infra/unix/systemd/eventra-cache-loader.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now eventra-api
systemctl enable --now eventra-rule-engine
systemctl enable --now eventra-cache-loader
```

### 7. Caddy'yi koy

```bash
cp /opt/eventra/current/infra/unix/Caddyfile /etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile
systemctl restart caddy
```

### 8. Son kontrol

```bash
curl http://127.0.0.1:3001/health
curl http://127.0.0.1:3002/health
curl http://127.0.0.1:3010/health
curl -I https://www.example.com
curl https://api.example.com/health
```

## Onerilen dizin yapisi

```text
/opt/eventra/current
/etc/eventra/eventra.env
/etc/systemd/system/eventra-api.service
/etc/systemd/system/eventra-rule-engine.service
/etc/systemd/system/eventra-cache-loader.service
/etc/caddy/Caddyfile
```

## 1. Sunucu paketleri

Ubuntu/Debian:

```bash
apt-get update
apt-get install -y curl git build-essential caddy redis-server
```

Node 20:

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs
```

Kafka'yi host servisi olarak ayri kurman gerekir.

## 2. Kodu yerlestir

```bash
mkdir -p /opt/eventra
git clone <repo-url> /opt/eventra/current
cd /opt/eventra/current
npm ci
```

## 3. Ortam dosyasi

Ortak env dosyasini olustur:

```bash
mkdir -p /etc/eventra
cp .env.example /etc/eventra/eventra.env
```

Asagidaki alanlari host adreslerine gore duzenle:

```bash
KAFKA_BROKERS=127.0.0.1:9092
DB_VENDOR=oracle
ORACLE_USER=eventra
ORACLE_PASSWORD=secret
ORACLE_CONNECT_STRING=127.0.0.1:1521/FREEPDB1
REDIS_URL=redis://127.0.0.1:6379
PORT=3001
CACHE_LOADER_PORT=3010
CACHE_LOADER_METADATA_DB_VENDOR=oracle
CACHE_LOADER_METADATA_ORACLE_USER=eventra
CACHE_LOADER_METADATA_ORACLE_PASSWORD=secret
CACHE_LOADER_METADATA_ORACLE_CONNECT_STRING=127.0.0.1:1521/FREEPDB1
RULE_ENGINE_HEALTH_PORT=3002
LLM_PROVIDER=mock
```

Oracle schema yuklemesi:

```bash
sqlplus eventra/secret@127.0.0.1:1521/FREEPDB1 @infra/unix/oracle-schema.sql
sqlplus eventra/secret@127.0.0.1:1521/FREEPDB1 @infra/unix/oracle-smoke-test.sql
ORACLE_USER=eventra ORACLE_PASSWORD=secret ORACLE_CONNECT_STRING=127.0.0.1:1521/FREEPDB1 ./infra/unix/run-oracle-smoke.sh
```

Frontend build icin gerekiyorsa:

```bash
APP_DOMAIN=www.example.com
API_DOMAIN=api.example.com
ACME_EMAIL=ops@example.com
```

## 4. Frontend build

```bash
cd /opt/eventra/current
VITE_API_BASE_URL=https://api.example.com npm --workspace @eventra/frontend run build
```

Static dosyalar:

```text
/opt/eventra/current/apps/frontend/dist
```

## 5. systemd unit dosyalari

Bu dizindeki unit dosyalarini hosta kopyala:

```bash
cp infra/unix/systemd/eventra-api.service /etc/systemd/system/
cp infra/unix/systemd/eventra-rule-engine.service /etc/systemd/system/
cp infra/unix/systemd/eventra-cache-loader.service /etc/systemd/system/
systemctl daemon-reload
```

## 6. Servisleri ayri ayri baslat

```bash
systemctl enable --now eventra-api
systemctl enable --now eventra-rule-engine
systemctl enable --now eventra-cache-loader
```

Durum:

```bash
systemctl status eventra-api --no-pager
systemctl status eventra-rule-engine --no-pager
systemctl status eventra-cache-loader --no-pager
```

Log:

```bash
journalctl -u eventra-api -f
journalctl -u eventra-rule-engine -f
journalctl -u eventra-cache-loader -f
```

Restart:

```bash
systemctl restart eventra-api
systemctl restart eventra-rule-engine
systemctl restart eventra-cache-loader
```

Stop:

```bash
systemctl stop eventra-api
systemctl stop eventra-rule-engine
systemctl stop eventra-cache-loader
```

## 7. Caddy

`infra/unix/Caddyfile` dosyasini kendi domainine gore duzenleyip `/etc/caddy/Caddyfile` olarak kopyala:

```bash
cp infra/unix/Caddyfile /etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile
systemctl restart caddy
```

## 8. Saglik kontrolleri

```bash
curl http://127.0.0.1:3001/health
curl http://127.0.0.1:3002/health
curl http://127.0.0.1:3010/health
curl -I https://www.example.com
curl https://api.example.com/health
```

## Notlar

- Bu modelde servisleri ayri ayri restart etmek kolaydir.
- `frontend` process olarak degil static site olarak servis edilir.
- Isteyen kurulumlarda `rule-engine` ve `cache-loader` ayri sunuculara da tasinabilir; sadece env adresleri degisir.
