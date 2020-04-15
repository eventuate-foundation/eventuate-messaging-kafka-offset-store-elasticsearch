package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("eventuate.local.kafka.elasticsearch")
public class EventuateKafkaConsumerElasticsearchSpringConfigurationProperties {
  private Map<String, String> properties = new HashMap<>();

  private String offsetStorageIndexName;
  private String offsetStorageTypeName;

  public String getOffsetStorageIndexName() {
    return offsetStorageIndexName;
  }

  public void setOffsetStorageIndexName(String offsetStorageIndexName) {
    this.offsetStorageIndexName = offsetStorageIndexName;
  }

  public String getOffsetStorageTypeName() {
    return offsetStorageTypeName;
  }

  public void setOffsetStorageTypeName(String offsetStorageTypeName) {
    this.offsetStorageTypeName = offsetStorageTypeName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
