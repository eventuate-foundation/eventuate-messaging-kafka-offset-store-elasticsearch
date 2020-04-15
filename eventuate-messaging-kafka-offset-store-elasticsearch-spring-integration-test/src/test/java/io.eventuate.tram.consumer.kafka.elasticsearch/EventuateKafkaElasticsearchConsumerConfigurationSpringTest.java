package io.eventuate.tram.consumer.kafka.elasticsearch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
    EventuateKafkaConsumerElasticsearchSpringConfigurationPropertiesConfiguration.class,
})
public class EventuateKafkaElasticsearchConsumerConfigurationSpringTest {

  @Autowired
  private EventuateKafkaConsumerElasticsearchSpringConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  @Test
  public void testPropertyParsing() {
    assertEquals(0, eventuateKafkaConsumerConfigurationProperties.getProperties().size());
    assertNull(eventuateKafkaConsumerConfigurationProperties.getProperties().get("offset-storage-index-name"));
    assertNull(eventuateKafkaConsumerConfigurationProperties.getProperties().get("offset-storage-type-name"));
  }
}
