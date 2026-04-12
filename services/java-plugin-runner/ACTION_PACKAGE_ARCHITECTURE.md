# Action Package Architecture

Yeni bir servise entegrasyon eklerken her aksiyonu kendi paketinde ayni yapida tut:

```text
actions/
  <service-name>/
    <ServiceName>Action.java
    mapper/
      <ServiceName>Mapper.java
    model/
      <ServiceName>Input.java
      <ServiceName>Output.java
    service/
      <ServiceName>ClientConfig.java
      <ServiceName>ServiceClient.java
      <ServiceName>ServiceResponse.java
```

## Sorumluluk dagilimi

- `*Action`:
  - env/config okur
  - mapper + service client cagirir
  - `PluginResult` dondurur
- `mapper/*`:
  - `PluginExecution` -> typed input
  - servis response -> typed output
  - output -> `Map<String,Object>` (rule-engine context icin)
- `service/*`:
  - HTTP/gRPC vb dis servis cagrilari
  - request/response parse
- `model/*`:
  - typed input/output record'lari

Bu yapi ile yeni bir servis eklemek icin `contactpolicy` paketini referans alip ayni katmanlari kopyalayarak ilerleyebilirsin.

