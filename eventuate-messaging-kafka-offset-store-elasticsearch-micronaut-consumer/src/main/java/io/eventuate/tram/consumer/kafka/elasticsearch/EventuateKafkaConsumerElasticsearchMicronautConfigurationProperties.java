package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("eventuate.local.kafka.elasticsearch")
public class EventuateKafkaConsumerElasticsearchMicronautConfigurationProperties {
  Map<String, String> properties = new HashMap<>();

  public Map<String, String> getProperties() {
    return properties
               .entrySet()
               .stream()
               .collect(Collectors.toMap(o -> o.getKey().replace("-", "."), Map.Entry::getValue));
  }
}
