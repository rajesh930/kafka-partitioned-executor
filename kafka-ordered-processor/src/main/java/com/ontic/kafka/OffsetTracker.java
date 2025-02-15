package com.ontic.kafka;

import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author rajesh
 * @since 14/02/25 11:04
 */
public class OffsetTracker<K, V> {
    private final Map<TopicPartition, Queue<RecordWithStatus<K, V>>> offsets;
    private final Counter expiryCounter;

    public OffsetTracker(Counter expiryCounter) {
        this.expiryCounter = expiryCounter;
        offsets = new HashMap<>();
    }

    public void enqueue(RecordWithStatus<K, V> record) {
        TopicPartition topicPartition = new TopicPartition(record.getRecord().topic(), record.getRecord().partition());
        offsets.computeIfAbsent(topicPartition, k -> new LinkedList<>()).add(record);
    }

    public Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
        Map<TopicPartition, OffsetAndMetadata> committableOffsets = new HashMap<>();
        long currentTime = System.currentTimeMillis();
        offsets.forEach((topicPartition, queue) -> {
            while (true) {
                RecordWithStatus<K, V> peek = queue.peek();
                if (peek == null) {
                    break;
                }
                if (peek.isComplete()) {
                    queue.poll();
                    committableOffsets.put(topicPartition, new OffsetAndMetadata(peek.getRecord().offset() + 1));
                } else if (peek.isExpired(currentTime)) {
                    expiryCounter.increment();
                    queue.poll();
                    committableOffsets.put(topicPartition, new OffsetAndMetadata(peek.getRecord().offset() + 1));
                } else {
                    break;
                }
            }
        });
        return committableOffsets;
    }
}
