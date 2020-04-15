package io.eventuate.tram.consumer.kafka.elasticsearch;

import javax.inject.Singleton;
import org.elasticsearch.client.RestHighLevelClient;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerConfigurer;
import io.micronaut.context.annotation.Factory;

@Factory
public class ElasticsearchKafkaConsumerConfigurerConfiguration {
    @Singleton
    KafkaConsumerConfigurer kafkaConsumerConfigurer(
        RestHighLevelClient client,
        ElasticsearchOffsetStorageConfigurationProperties properties) {
        return new ElasticsearchKafkaConsumerConfigurer(client, properties);
    }
}
