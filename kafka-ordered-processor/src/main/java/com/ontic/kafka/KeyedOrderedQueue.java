package com.ontic.kafka;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author rajesh
 * @since 13/02/25 12:00
 */
public class KeyedOrderedQueue<K, V> {

    private final ConcurrentHashMap<K, Queue<V>> queues;
    private final AtomicInteger messagesInQueue = new AtomicInteger(0);

    public KeyedOrderedQueue(int capacity) {
        this.queues = new ConcurrentHashMap<>(capacity);
    }

    /**
     * If queue is already present adds message to the queue else creates the queue and add the message.
     *
     * @param key     key to the queue to add message to it
     * @param message message to add
     * @return true if queue was created then message was added, false if message was added to existing queue
     */
    public boolean enqueueMessage(K key, V message) {
        AtomicBoolean added = new AtomicBoolean(false);
        queues.compute(key, (k, vs) -> {
            if (vs == null) {
                vs = new LinkedList<>();
                added.set(true);
            }
            vs.add(message);
            messagesInQueue.incrementAndGet();
            return vs;
        });
        return added.get();
    }

    /**
     * Dequeue message for the given key. If no message available on queue, queue is atomically removed.
     * If this method return null, same thread should not try to dequeue for intended use-case where new thread takes over processing
     * of messages for given key
     *
     * @param key key to the queue to remove message from
     * @return the dequeued message, null if not message available
     */
    public V dequeueMessage(K key) {
        AtomicReference<V> removed = new AtomicReference<>(null);
        queues.compute(key, (k, vs) -> {
            if (vs == null || vs.isEmpty()) {
                return null;
            }
            V message = vs.poll();
            removed.set(message);
            messagesInQueue.decrementAndGet();
            return vs;
        });
        return removed.get();
    }

    /**
     * Current number of message in across all queues
     *
     * @return Current number of message in across all queues
     */
    public int messagesInQueue() {
        return messagesInQueue.get();
    }
}
