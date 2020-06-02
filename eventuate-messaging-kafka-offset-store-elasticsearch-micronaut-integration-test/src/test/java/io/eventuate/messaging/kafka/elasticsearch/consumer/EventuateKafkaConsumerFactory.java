package io.eventuate.messaging.kafka.elasticsearch.consumer;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.tram.consumer.kafka.elasticsearch.ElasticsearchConstants;
import io.eventuate.tram.consumer.kafka.elasticsearch.ElasticsearchKafkaConsumerFactory;
import io.eventuate.tram.consumer.kafka.elasticsearch.ElasticsearchOffsetStorageConfigurationProperties;
import io.micronaut.context.annotation.Factory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Factory
public class EventuateKafkaConsumerFactory {
  @Singleton
  public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties, EventuateKafkaProducerConfigurationProperties producerProperties) {
    return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
  }

  @Singleton
  public MessageConsumerKafkaImpl messageConsumerKafka(EventuateKafkaConfigurationProperties props,
                                                       EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                       KafkaConsumerFactory kafkaConsumerFactory) {
    return new MessageConsumerKafkaImpl(props.getBootstrapServers(), eventuateKafkaConsumerConfigurationProperties, kafkaConsumerFactory);
  }

  @Singleton
  public KafkaConsumerFactory kafkaConsumerFactory(RestHighLevelClient client, ElasticsearchOffsetStorageConfigurationProperties properties) {
    return new ElasticsearchKafkaConsumerFactory(client, properties);
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
    return new HttpHost(System.getenv("DOCKER_HOST_IP"), ElasticsearchConstants.DEFAULT_PORT);
  }
}
