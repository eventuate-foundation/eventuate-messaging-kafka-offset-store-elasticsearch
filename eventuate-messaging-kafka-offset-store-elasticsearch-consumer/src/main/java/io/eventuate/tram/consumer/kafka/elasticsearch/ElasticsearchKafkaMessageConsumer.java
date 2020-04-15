package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class ElasticsearchKafkaMessageConsumer implements KafkaMessageConsumerWithOffsetStorageSupport {

    public static final int SIZE = 10000;

    private static final String STORE_ERROR_MESSAGE = "Error storing the offsets in elasticsearch";
    private static final String READ_ERROR_MESSAGE = "Error reading the offsets from elasticsearch";

    private final RestHighLevelClient client;
    private final ElasticsearchOffsetStorageConfigurationProperties properties;
    private final String subscriptionId;

    private final KafkaConsumer<String, byte[]> kafkaConsumer;

    private final static ObjectMapper MAPPER =
        new ObjectMapper().registerModule(
            new SimpleModule()
                .addSerializer(OffsetStorageDocument.class, new OffsetStorageDocumentSerializer())
                .addDeserializer(OffsetStorageDocument.class, new OffsetStorageDocumentDeserializer())
        );

    public ElasticsearchKafkaMessageConsumer(
        String subscriptionId,
        KafkaConsumer<String, byte[]> kafkaConsumer,
        RestHighLevelClient client,
        ElasticsearchOffsetStorageConfigurationProperties properties) {
        this.subscriptionId = subscriptionId;
        this.client = client;
        this.properties = properties;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            if (offsets.isEmpty()) {
                return;
            }
            kafkaConsumer.commitSync(offsets);
            BulkRequest request = new BulkRequest();
            offsets.forEach((partition, offsetAndMetadata) -> {
                try {
                    OffsetStorageDocument document = new OffsetStorageDocument(partition, offsetAndMetadata, subscriptionId);
                    request.add(new IndexRequest()
                                    .index(properties.getOffsetStorageIndexName())
                                    .type(properties.getOffsetStorageTypeName())
                                    .id(document.id())
                                    .source(MAPPER.writeValueAsString(document), XContentType.JSON));
                } catch (IOException e) {
                    throw new OffsetStorageException(STORE_ERROR_MESSAGE, e);
                }
            });
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                throw new OffsetStorageException(String.format("%s. Details:\n%s", STORE_ERROR_MESSAGE, response.buildFailureMessage()));
            }
        } catch (IOException e) {
            throw new OffsetStorageException(STORE_ERROR_MESSAGE, e);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> readOffsets() {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            SearchResponse response = client.search(
                new SearchRequest()
                    .source(new SearchSourceBuilder()
                                .size(SIZE)
                                .query(QueryBuilders.matchQuery(OffsetStorageDocument.SUBSCRIPTION_ID, subscriptionId)))
                    .indices(properties.getOffsetStorageIndexName()),
                RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits().getHits()) {
                OffsetStorageDocument offsetDocument = MAPPER.readValue(hit.getSourceAsString(), OffsetStorageDocument.class);
                offsets.put(offsetDocument.partition, offsetDocument.offsetAndMetadata);
            }
            return offsets;
        } catch (IOException e) {
            throw new UncheckedIOException(READ_ERROR_MESSAGE, e);
        }
    }

    @Override
    public void subscribe(List<String> topics) {
        kafkaConsumer.subscribe(topics, new SaveOffsetOnRebalanceListener(this));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaConsumer.partitionsFor(topic);
    }

    @Override
    public ConsumerRecords<String, byte[]> poll(Duration duration) {
        return kafkaConsumer.poll(duration);
    }

    @Override
    public void pause(Set<TopicPartition> partitions) {
        kafkaConsumer.pause(partitions);
    }

    @Override
    public void resume(Set<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

    @Override
    public void close(Duration duration) {
        kafkaConsumer.close(duration);
    }

    @Override
    public long position(TopicPartition partition) {
        return kafkaConsumer.position(partition);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata position) {
        kafkaConsumer.seek(partition, position);
    }

    private final static class OffsetStorageDocument {

        static final String SUBSCRIPTION_ID = "subscriptionId";
        static final String TOPIC = "topic";
        static final String PARTITION = "partition";
        static final String OFFSET = "offset";
        static final String METADATA = "metadata";

        TopicPartition partition;
        OffsetAndMetadata offsetAndMetadata;

        String subscriptionId;

        public OffsetStorageDocument(TopicPartition partition, OffsetAndMetadata offsetAndMetadata, String subscriptionId) {
            this.partition = partition;
            this.offsetAndMetadata = offsetAndMetadata;
            this.subscriptionId = subscriptionId;
        }

        public String id() {
            return String.format("%s-%s-%s", subscriptionId, partition.topic(), partition.partition());
        }
    }

    private final static class OffsetStorageDocumentSerializer extends StdSerializer<OffsetStorageDocument> {

        public OffsetStorageDocumentSerializer() {
            this(null);
        }

        protected OffsetStorageDocumentSerializer(Class<OffsetStorageDocument> t) {
            super(t);
        }

        @Override
        public void serialize(OffsetStorageDocument document, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeStringField(OffsetStorageDocument.SUBSCRIPTION_ID, document.subscriptionId);
            jgen.writeStringField(OffsetStorageDocument.TOPIC, document.partition.topic());
            jgen.writeNumberField(OffsetStorageDocument.PARTITION, document.partition.partition());
            jgen.writeNumberField(OffsetStorageDocument.OFFSET, document.offsetAndMetadata.offset());
            jgen.writeStringField(OffsetStorageDocument.METADATA, document.offsetAndMetadata.metadata());
            jgen.writeEndObject();
        }
    }

    static final class OffsetStorageDocumentDeserializer extends StdDeserializer<OffsetStorageDocument> {

        public OffsetStorageDocumentDeserializer() {
            this(null);
        }

        public OffsetStorageDocumentDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public OffsetStorageDocument deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            String topic = node.get(OffsetStorageDocument.TOPIC).asText();
            int partition = node.get(OffsetStorageDocument.PARTITION).numberValue().intValue();
            long offset = node.get(OffsetStorageDocument.OFFSET).numberValue().longValue();
            String metadata = node.get(OffsetStorageDocument.METADATA).asText();
            String subscriptionId = node.get(OffsetStorageDocument.SUBSCRIPTION_ID).asText();
            return new OffsetStorageDocument(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, metadata), subscriptionId);
        }
    }
}
