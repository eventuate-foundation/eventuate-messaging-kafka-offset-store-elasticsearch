package io.eventuate.tram.consumer.kafka.elasticsearch;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SaveOffsetOnRebalanceListener implements ConsumerRebalanceListener {

  private static final Logger logger = LoggerFactory.getLogger(SaveOffsetOnRebalanceListener.class);

  private final KafkaMessageConsumerWithOffsetStorageSupport consumer;

  public SaveOffsetOnRebalanceListener(KafkaMessageConsumerWithOffsetStorageSupport consumer) {
    this.consumer = consumer;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    logger.debug("Partitions revoked. Committing offsets...");
    Map<TopicPartition, OffsetAndMetadata> offsets =
            partitions.stream()
                    .collect(Collectors.toMap(Function.identity(), partition -> new OffsetAndMetadata(consumer.position(partition))));
    consumer.commitOffsets(offsets);
    logger.debug("Partitions revoked. Offsets committed.");
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    logger.debug("Partitions assigned. Reading offsets and seeking positions...");
    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.readOffsets();
    for (TopicPartition partition : partitions) {
      Optional.ofNullable(offsets.get(partition)).ifPresent(position -> {
        consumer.seek(partition, position);
      });
    }
    logger.debug("Partitions assigned. Offsets read and position sought.");
  }
}
