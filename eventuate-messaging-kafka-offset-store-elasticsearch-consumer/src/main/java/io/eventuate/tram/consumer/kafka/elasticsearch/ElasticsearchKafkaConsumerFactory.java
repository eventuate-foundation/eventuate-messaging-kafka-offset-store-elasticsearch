package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.RestHighLevelClient;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.basic.consumer.KafkaMessageConsumer;

public class ElasticsearchKafkaConsumerFactory implements KafkaConsumerFactory {

  private final RestHighLevelClient client;
  private final ElasticsearchOffsetStorageConfigurationProperties properties;

  public ElasticsearchKafkaConsumerFactory(RestHighLevelClient client,
                                           ElasticsearchOffsetStorageConfigurationProperties properties) {
    this.client = client;
    this.properties = properties;
  }

  @Override
  public KafkaMessageConsumer makeConsumer(String subscriptionId, Properties consumerProperties) {
    return new ElasticsearchKafkaMessageConsumer(
            subscriptionId,
            new KafkaConsumer<>(consumerProperties),
            client,
            properties);
  }
}
