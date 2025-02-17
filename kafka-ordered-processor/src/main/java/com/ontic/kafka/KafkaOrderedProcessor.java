package com.ontic.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

/**
 * @author rajesh
 * @since 27/01/25 17:04
 */
public class KafkaOrderedProcessor<K, V> extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(KafkaOrderedProcessor.class);

    private final KafkaConfig<K, V> kafkaConfig;
    private final KeyedOrderedQueue<K, RecordWithStatus<K, V>> orderedQueue;
    private final Consumer<V> messageHandler;
    private final ExecutorService executor;
    private final OffsetTracker<K, V> offsetTracker;
    private KafkaConsumer<K, V> consumer;

    private Counter messagesProcessed;
    private Counter messagesFailed;
    private Timer messagesProcessTimer;
    private Counter messagesExpired;

    public KafkaOrderedProcessor(KafkaConfig<K, V> kafkaConfig, Consumer<V> messageHandler) {
        super("kafka.ordered.processor." + kafkaConfig.getTopic());
        this.kafkaConfig = kafkaConfig;
        this.orderedQueue = new KeyedOrderedQueue<>(kafkaConfig.getMaxQueuedMessages());
        this.messageHandler = messageHandler;
        this.executor = Executors.newCachedThreadPool();
        registerMetrics();
        this.offsetTracker = new OffsetTracker<>(messagesExpired);
    }

    /**
     * Called internally by thread, call start() method
     */
    public void run() {
        Properties properties = kafkaConfig.toProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(properties, kafkaConfig.getKeyDeserializer(), kafkaConfig.getValueDeserializer());
        this.consumer.subscribe(Collections.singleton(kafkaConfig.getTopic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                if (logger.isInfoEnabled()) {
                    logger.info("Partitions revoked: {}", collection);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                if (logger.isInfoEnabled()) {
                    logger.info("Partitions assigned: {}", collection);
                }
            }
        });
        while (true) {
            try {
                ConsumerRecords<K, V> records = this.consumer.poll(kafkaConfig.getPollTimeout());
                long expectedCompletionTime = System.currentTimeMillis() + kafkaConfig.getMaxMessageProcessingTime().toMillis();
                for (ConsumerRecord<K, V> record : records) {
                    RecordWithStatus<K, V> message = new RecordWithStatus<>(record, expectedCompletionTime);
                    offsetTracker.enqueue(message);
                    boolean queueCreated = this.orderedQueue.enqueueMessage(record.key(), message);
                    if (queueCreated) {
                        executor.submit(new KeyedOrderedDelegator<>(record.key(), orderedQueue, recordWithStatus -> {
                            try {
                                long startTime = System.nanoTime();

                                messageHandler.accept(recordWithStatus.getRecord().value());

                                messagesProcessed.increment();

                                long duration = System.nanoTime() - startTime;
                                messagesProcessTimer.record(duration, TimeUnit.NANOSECONDS);
                            } catch (Throwable t) {
                                messagesFailed.increment();
                                logger.error("Error while processing record", t);
                            } finally {
                                recordWithStatus.markCompleted();
                            }
                        }));
                    }
                }
                Map<TopicPartition, OffsetAndMetadata> committableOffsets = offsetTracker.getCommittableOffsets();
                if (!committableOffsets.isEmpty()) {
                    this.consumer.commitSync(committableOffsets);
                }
                while (orderedQueue.messagesInQueue() > kafkaConfig.getMaxQueuedMessages()) {
                    try {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Max number of messages is in queue: {}, sleeping for a sec", orderedQueue.messagesInQueue());
                        }
                        //noinspection BusyWait
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } catch (WakeupException e) {
                if (logger.isInfoEnabled()) {
                    logger.info("WakeupException occurred while polling records, shutting down.");
                }
                break;
            } catch (Throwable t) {
                logger.error("Unexpected error occurred while polling records, continuing...", t);
            }
        }
    }

    public void shutdown(Duration timeout) throws InterruptedException {
        if (this.consumer == null) {
            return;
        }
        try {
            this.consumer.wakeup();
            this.executor.shutdown();
            boolean terminated = this.executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!terminated) {
                logger.error("Message Processor failed to terminate properly, some message may not have processed");
            }
            this.join(timeout.toMillis());
        } finally {
            Map<TopicPartition, OffsetAndMetadata> committableOffsets = offsetTracker.getCommittableOffsets();
            if (!committableOffsets.isEmpty()) {
                this.consumer.commitSync(committableOffsets);
            }
            this.consumer.close();
        }
    }

    private void registerMetrics() {
        this.messagesProcessed = Counter.builder("messages.processed").tag("topic", kafkaConfig.getTopic()).register(globalRegistry);
        this.messagesFailed = Counter.builder("messages.failed").tag("topic", kafkaConfig.getTopic()).register(globalRegistry);
        this.messagesProcessTimer = Timer.builder("messages.process.time").tag("topic", kafkaConfig.getTopic()).register(globalRegistry);
        this.messagesExpired = Counter.builder("messages.expired").tag("topic", kafkaConfig.getTopic()).register(globalRegistry);
        Gauge.builder("messages.in.queue", orderedQueue::messagesInQueue).tag("topic", kafkaConfig.getTopic()).register(globalRegistry);
    }
}
