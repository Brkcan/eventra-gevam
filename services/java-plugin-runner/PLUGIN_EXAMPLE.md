# Java Plugin Example

Bu ornek, `java-plugin-runner` ile calisacak bir plugin JAR iskeletini gosterir.

## 1. Interface bagimliligi

Plugin projesi, `JourneyActionPlugin` interface'ini gormelidir.
MVP icin en kolay yol:
- runner projesini localde `mvn install` ile m2 reposuna koy
- plugin projesinde bu artifact'i dependency olarak kullan

## 2. Plugin sinifi

```java
package com.acme.eventra;

import com.eventra.pluginrunner.api.JourneyActionPlugin;
import com.eventra.pluginrunner.api.PluginExecution;
import com.eventra.pluginrunner.api.PluginResult;
import java.util.Map;

public final class KafkaProducePlugin implements JourneyActionPlugin {
  @Override
  public String id() {
    return "acme.kafka.produce.v1";
  }

  @Override
  public String displayName() {
    return "Acme Kafka Produce";
  }

  @Override
  public PluginResult execute(PluginExecution execution) {
    // Burada kendi Java Kafka producer kodunu calistir.
    // execution.getActionConfig(), execution.getEvent() vb alanlar mevcut.
    return PluginResult.ok("message produced", Map.of("topic", "acme.out"));
  }
}
```

## 3. ServiceLoader kaydi

`META-INF/services/com.eventra.pluginrunner.api.JourneyActionPlugin` dosyasina
tam sinif adini yaz:

```text
com.acme.eventra.KafkaProducePlugin
```

## 4. Deploy

- Uretilen JAR dosyasini runner'in plugin klasorune koy (`JAVA_PLUGIN_DIR`, default `plugins`).
- Runner'da reload cagir:

```bash
curl -X POST http://127.0.0.1:3030/plugins/reload
curl http://127.0.0.1:3030/plugins/actions
```

