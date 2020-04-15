package io.eventuate.tram.consumer.kafka.elasticsearch;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@EnableConfigurationProperties(EventuateKafkaConsumerElasticsearchSpringConfigurationProperties.class)
public class EventuateKafkaConsumerElasticsearchSpringConfigurationPropertiesConfiguration {

    @Bean
    ElasticsearchOffsetStorageConfigurationProperties eventuateKafkaConsumerElasticsearchSpringConfigurationProperties(EventuateKafkaConsumerElasticsearchSpringConfigurationProperties eventuateKafkaConsumerElasticsearchSpringConfigurationProperties) {
        ElasticsearchOffsetStorageConfigurationProperties properties = new ElasticsearchOffsetStorageConfigurationProperties(eventuateKafkaConsumerElasticsearchSpringConfigurationProperties.getProperties());
        properties.setOffsetStorageIndexName(eventuateKafkaConsumerElasticsearchSpringConfigurationProperties.getOffsetStorageIndexName());
        properties.setOffsetStorageTypeName(eventuateKafkaConsumerElasticsearchSpringConfigurationProperties.getOffsetStorageTypeName());
        return properties;
    }
}
