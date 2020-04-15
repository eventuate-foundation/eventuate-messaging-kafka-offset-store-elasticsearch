package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class SaveOffsetOnRebalanceListener implements ConsumerRebalanceListener {

    private final KafkaMessageConsumerWithOffsetStorageSupport consumer;

    public SaveOffsetOnRebalanceListener(KafkaMessageConsumerWithOffsetStorageSupport consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> offsets =
            partitions.stream()
                      .collect(Collectors.toMap(Function.identity(), partition -> new OffsetAndMetadata(consumer.position(partition))));
        consumer.commitOffsets(offsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> offsets = consumer.readOffsets();
        for (TopicPartition partition : partitions) {
            Optional.ofNullable(offsets.get(partition)).ifPresent(position -> {
                consumer.seek(partition, position);
            });
        }
    }
}
