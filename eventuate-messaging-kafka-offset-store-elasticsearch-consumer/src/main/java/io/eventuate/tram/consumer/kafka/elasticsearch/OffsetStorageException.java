package io.eventuate.tram.consumer.kafka.elasticsearch;

public class OffsetStorageException extends RuntimeException {
    public OffsetStorageException(String message) {
        super(message);
    }

    public OffsetStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public OffsetStorageException(Throwable cause) {
        super(cause);
    }
}
