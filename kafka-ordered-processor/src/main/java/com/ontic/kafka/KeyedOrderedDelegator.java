package com.ontic.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * @author rajesh
 * @since 13/02/25 16:58
 */
public class KeyedOrderedDelegator<K, V> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KeyedOrderedDelegator.class);

    private final K key;
    private final KeyedOrderedQueue<K, V> queue;
    private final Consumer<V> consumer;

    public KeyedOrderedDelegator(K key, KeyedOrderedQueue<K, V> queue, Consumer<V> consumer) {
        this.key = key;
        this.queue = queue;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        for (; ; ) {
            V message = queue.dequeueMessage(key);
            if (message != null) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing message: {}", message);
                    }
                    consumer.accept(message);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processed message: {}", message);
                    }
                } catch (Exception e) {
                    logger.error("Error Processing message: {}", message, e);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Exiting: {}", key);
                }
                break;
            }
        }
    }
}
