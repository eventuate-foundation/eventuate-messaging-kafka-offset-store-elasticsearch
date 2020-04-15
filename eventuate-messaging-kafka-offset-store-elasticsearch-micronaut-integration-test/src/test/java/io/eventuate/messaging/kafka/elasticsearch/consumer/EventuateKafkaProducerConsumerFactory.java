package io.eventuate.messaging.kafka.elasticsearch.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.inject.Singleton;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerConfigurer;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.tram.consumer.kafka.elasticsearch.ElasticsearchConstants;
import io.eventuate.tram.consumer.kafka.elasticsearch.ElasticsearchKafkaConsumerConfigurer;
import io.eventuate.tram.consumer.kafka.elasticsearch.ElasticsearchOffsetStorageConfigurationProperties;
import io.micronaut.context.annotation.Factory;

@Factory
public class EventuateKafkaProducerConsumerFactory {
  @Singleton
  public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties, EventuateKafkaProducerConfigurationProperties producerProperties) {
    return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
  }

  @Singleton
  public MessageConsumerKafkaImpl messageConsumerKafka(EventuateKafkaConfigurationProperties props,
                                                       EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                       KafkaConsumerConfigurer kafkaConsumerConfigurer) {
    return new MessageConsumerKafkaImpl(props.getBootstrapServers(), eventuateKafkaConsumerConfigurationProperties, kafkaConsumerConfigurer);
  }

  @Singleton
  public KafkaConsumerConfigurer kafkaConsumerConfigurer(RestHighLevelClient client, ElasticsearchOffsetStorageConfigurationProperties properties) {
    return new ElasticsearchKafkaConsumerConfigurer(client, properties);
  }

  @Singleton
  public RestHighLevelClient restHighLevelClient() {
    return new RestHighLevelClient(RestClient.builder(getElasticsearchHttpHost()));
  }

  @Singleton
  public ElasticsearchOffsetStorageConfigurationProperties elasticsearchOffsetStorageConfigurationProperties() {
    ElasticsearchOffsetStorageConfigurationProperties elasticsearchOffsetStorageConfigurationProperties = new ElasticsearchOffsetStorageConfigurationProperties();
    elasticsearchOffsetStorageConfigurationProperties.setOffsetStorageIndexName("offsets");
    elasticsearchOffsetStorageConfigurationProperties.setOffsetStorageTypeName("_doc");
    return elasticsearchOffsetStorageConfigurationProperties;
  }

  private static HttpHost getElasticsearchHttpHost() {
    try {
      return new HttpHost(InetAddress.getLocalHost(), ElasticsearchConstants.DEFAULT_PORT);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
