package io.eventuate.tram.consumer.kafka.elasticsearch;

import io.eventuate.messaging.kafka.basic.consumer.AbstractEventuateKafkaBasicConsumerTest;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.basic.consumer.EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = EventuateKafkaElasticsearchConsumerTest.EventuateKafkaConsumerTestConfiguration.class,
        properties = "eventuate.local.kafka.consumer.backPressure.high=3"
)
public class EventuateKafkaElasticsearchConsumerTest extends AbstractEventuateKafkaBasicConsumerTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaPropertiesConfiguration.class})
  public static class EventuateKafkaConsumerTestConfiguration {

    @Bean
    public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties,
                                           EventuateKafkaProducerConfigurationProperties producerProperties) {
      return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
    }

    @Bean
    public MessageConsumerKafkaImpl messageConsumerKafka(EventuateKafkaConfigurationProperties props,
                                                         EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                         KafkaConsumerFactory kafkaConsumerFactory) {
      return new MessageConsumerKafkaImpl(props.getBootstrapServers(), eventuateKafkaConsumerConfigurationProperties, kafkaConsumerFactory);
    }

    @Bean
    public KafkaConsumerFactory kafkaConsumerFactory(RestHighLevelClient client, ElasticsearchOffsetStorageConfigurationProperties properties) {
      return new ElasticsearchKafkaConsumerFactory(client, properties);
    }

    @Bean
    public RestHighLevelClient restHighLevelClient() {
      return new RestHighLevelClient(RestClient.builder(getElasticsearchHttpHost()));
    }

    @Bean
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

  @Autowired
  private EventuateKafkaConfigurationProperties kafkaProperties;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties consumerProperties;

  @Autowired
  private EventuateKafkaProducer producer;

  @Autowired
  private MessageConsumerKafkaImpl consumer;

  @Autowired
  private KafkaConsumerFactory factory;

  @Autowired
  ElasticsearchOffsetStorageConfigurationProperties properties;

  @Autowired
  private RestHighLevelClient elasticsearchClient;

  @Before
  public void setup() throws IOException {
    if (!elasticsearchClient.indices().exists(new GetIndexRequest(properties.getOffsetStorageIndexName()), RequestOptions.DEFAULT)) {
      elasticsearchClient.indices().create(new CreateIndexRequest(properties.getOffsetStorageIndexName()), RequestOptions.DEFAULT);
    }
  }

  @Test
  @Override
  public void shouldStopWhenHandlerThrowsException() {
    super.shouldStopWhenHandlerThrowsException();
  }

  @Test
  @Override
  public void shouldConsumeMessages() {
    super.shouldConsumeMessages();
  }

  @Test
  @Override
  public void shouldConsumeMessagesWithBackPressure() {
    super.shouldConsumeMessagesWithBackPressure();
  }

  @Test
  @Override
  public void shouldConsumeBatchOfMessage() {
    super.shouldConsumeBatchOfMessage();
  }

  @Override
  protected EventuateKafkaConfigurationProperties getKafkaProperties() {
    return kafkaProperties;
  }

  @Override
  protected EventuateKafkaConsumerConfigurationProperties getConsumerProperties() {
    return consumerProperties;
  }

  @Override
  protected EventuateKafkaProducer getProducer() {
    return producer;
  }

  @Override
  protected MessageConsumerKafkaImpl getConsumer() {
    return consumer;
  }

  @Override
  protected KafkaConsumerFactory getKafkaConsumerFactory() {
    return factory;
  }

}