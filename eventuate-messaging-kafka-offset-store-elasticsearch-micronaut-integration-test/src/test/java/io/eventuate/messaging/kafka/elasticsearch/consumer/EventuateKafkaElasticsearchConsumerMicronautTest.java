package io.eventuate.messaging.kafka.elasticsearch.consumer;

import javax.inject.Inject;
import org.junit.jupiter.api.Test;
import io.eventuate.messaging.kafka.basic.consumer.AbstractEventuateKafkaBasicConsumerTest;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerConfigurer;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.annotation.MicronautTest;

@MicronautTest
@Property(name = "eventuate.local.kafka.consumer.backPressure.high", value = "3")
public class EventuateKafkaElasticsearchConsumerMicronautTest extends AbstractEventuateKafkaBasicConsumerTest {

  @Inject
  private EventuateKafkaConfigurationProperties kafkaProperties;

  @Inject
  private EventuateKafkaConsumerConfigurationProperties consumerProperties;

  @Inject
  private EventuateKafkaProducer producer;

  @Inject
  private MessageConsumerKafkaImpl consumer;

  @Inject
  private KafkaConsumerConfigurer configurer;

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
  protected KafkaConsumerConfigurer getConfigurer() {
    return configurer;
  }
}