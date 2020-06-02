package io.eventuate.tram.consumer.kafka.elasticsearch;

import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.micronaut.context.annotation.Factory;
import org.elasticsearch.client.RestHighLevelClient;

import javax.inject.Singleton;

@Factory
public class ElasticsearchKafkaConsumerFactoryFactoryConfiguration {
  @Singleton
  KafkaConsumerFactory kafkaConsumerFactory(RestHighLevelClient client,
                                            ElasticsearchOffsetStorageConfigurationProperties properties) {
    return new ElasticsearchKafkaConsumerFactory(client, properties);
  }
}
