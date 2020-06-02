package io.eventuate.tram.consumer.kafka.elasticsearch;

import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchKafkaConsumerFactorySpringConfiguration {
  @Bean
  KafkaConsumerFactory kafkaConsumerFactory(
          RestHighLevelClient client,
          ElasticsearchOffsetStorageConfigurationProperties properties) {
    return new ElasticsearchKafkaConsumerFactory(client, properties);
  }
}
