package com.ontic.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author rajesh
 * @since 14/02/25 11:48
 */
public class RecordWithStatus<K, V> {
    private final ConsumerRecord<K, V> record;
    private final long expectedCompletionTime;
    private boolean complete = false;

    public RecordWithStatus(ConsumerRecord<K, V> record, long expectedCompletionTime) {
        this.record = record;
        this.expectedCompletionTime = expectedCompletionTime;
    }

    public ConsumerRecord<K, V> getRecord() {
        return record;
    }

    public void markCompleted() {
        complete = true;
    }

    public boolean isComplete() {
        return complete;
    }

    public boolean isExpired(long currentTime) {
        return expectedCompletionTime < currentTime;
    }
}
