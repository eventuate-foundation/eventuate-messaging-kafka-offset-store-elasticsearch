package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import io.eventuate.messaging.kafka.basic.consumer.KafkaMessageConsumer;

public interface KafkaMessageConsumerWithOffsetStorageSupport extends KafkaMessageConsumer {

    Map<TopicPartition, OffsetAndMetadata> readOffsets();

    long position(TopicPartition partition);

    void seek(TopicPartition partition, OffsetAndMetadata position);
}
