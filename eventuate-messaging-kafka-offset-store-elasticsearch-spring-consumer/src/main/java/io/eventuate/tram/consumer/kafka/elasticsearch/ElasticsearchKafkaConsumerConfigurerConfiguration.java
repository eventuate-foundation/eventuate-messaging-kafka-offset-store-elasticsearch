package io.eventuate.tram.consumer.kafka.elasticsearch;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerConfigurer;

@Configuration
public class ElasticsearchKafkaConsumerConfigurerConfiguration {
    @Bean
    KafkaConsumerConfigurer kafkaConsumerConfigurer(
        RestHighLevelClient client,
        ElasticsearchOffsetStorageConfigurationProperties properties) {
        return new ElasticsearchKafkaConsumerConfigurer(client, properties);
    }
}
