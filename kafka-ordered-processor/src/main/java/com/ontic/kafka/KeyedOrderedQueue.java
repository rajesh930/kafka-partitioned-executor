package com.ontic.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author rajesh
 * @since 13/02/25 12:00
 */
public class KeyedOrderedQueue<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(KeyedOrderedQueue.class);

    private final ConcurrentHashMap<K, Queue<V>> queues;
    private int messagesInQueue = 0;

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
            return vs;
        });
        messagesInQueue++;
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
        Queue<V> queue = queues.get(key);
        if (queue == null) {
            logger.error("Should not happen, unless same key [ {} ] is being used in multiple thread", key);
            return null;
        }
        V message = queue.poll();
        if (message == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Queue for key [ {} ] is empty, removing it", key);
            }
            boolean removed = removeIfEmpty(key);
            if (removed) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Queue for key [ {} ] removed", key);
                }
                return null;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Queue not removed some other message for key [ {} ] arrived in meantime", key);
                }
                return dequeueMessage(key);
            }
        }
        messagesInQueue--;
        return message;
    }

    /**
     * Returns true if removed else false. Checks if queue against key is empty then remove it otherwise leave as it is.
     *
     * @param key to remove
     * @return true if removed else false
     */
    private boolean removeIfEmpty(K key) {
        Queue<V> computed = queues.compute(key, (k, vs) -> {
            if (vs == null || vs.isEmpty()) {
                return null;
            } else {
                return vs;
            }
        });
        return computed == null;
    }

    /**
     * Current number of message in across all queues
     *
     * @return Current number of message in across all queues
     */
    public int messagesInQueue() {
        return messagesInQueue;
    }
}
