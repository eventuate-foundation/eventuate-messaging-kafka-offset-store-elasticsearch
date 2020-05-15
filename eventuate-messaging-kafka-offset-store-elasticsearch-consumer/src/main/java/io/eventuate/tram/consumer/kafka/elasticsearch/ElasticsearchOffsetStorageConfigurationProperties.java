package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ElasticsearchOffsetStorageConfigurationProperties {

  private static final String DEFAULT_OFFSET_STORAGE_INDEX_NAME = "offsets";
  private static final String DEFAULT_OFFSET_STORAGE_TYPE_NAME = "_doc";

  private Map<String, String> properties = new HashMap<>();

  private String offsetStorageIndexName;
  private String offsetStorageTypeName;

  public String getOffsetStorageIndexName() {
    return Optional.ofNullable(offsetStorageIndexName).orElse(DEFAULT_OFFSET_STORAGE_INDEX_NAME);
  }

  public void setOffsetStorageIndexName(String offsetStorageIndexName) {
    this.offsetStorageIndexName = offsetStorageIndexName;
  }

  public String getOffsetStorageTypeName() {
    return Optional.ofNullable(offsetStorageTypeName).orElse(DEFAULT_OFFSET_STORAGE_TYPE_NAME);
  }

  public void setOffsetStorageTypeName(String offsetStorageTypeName) {
    this.offsetStorageTypeName = offsetStorageTypeName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public ElasticsearchOffsetStorageConfigurationProperties() {
  }

  public ElasticsearchOffsetStorageConfigurationProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public static ElasticsearchOffsetStorageConfigurationProperties empty() {
    return new ElasticsearchOffsetStorageConfigurationProperties();
  }

}
