package com.ontic.kafka.test;

import com.ontic.kafka.KafkaConfig;
import com.ontic.kafka.KafkaOrderedProcessor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author rajesh
 * @since 17/02/25 12:02
 */
@SpringBootTest(classes = {KafkaTestApplication.class})
@TestPropertySource(locations = "classpath:kafka-ordered-processor-test.properties")
public class KafkaOrderedProcessorTest {

    @Autowired
    private KafkaConfig<String, String> kafkaConfig;

    @Test
    public void testMessagesAreProcessedInOrderForEachPartition() throws Exception {
        createTopic();
        int partitions = 100;
        int messagesPerPartition = 200;
        produceMessages(partitions, messagesPerPartition);

        kafkaConfig.setKeyDeserializer(new StringDeserializer());
        kafkaConfig.setValueDeserializer(new StringDeserializer());
        AtomicInteger processed = new AtomicInteger(0);
        ConcurrentHashMap<String, Integer> keyVsMessNumber = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> keyVsErrors = new ConcurrentHashMap<>();

        KafkaOrderedProcessor<String, String> kafkaOrderedProcessor = new KafkaOrderedProcessor<>(kafkaConfig, s -> {
            processed.incrementAndGet();
            String[] split = s.split(":");
            String key = split[0];
            int value = Integer.parseInt(split[1]);
            keyVsMessNumber.compute(key, (s1, prev) -> {
                if (prev == null) {
                    prev = -1;
                }
                if (value - prev != 1) {
                    keyVsErrors.put(key, "Out of order " + value);
                }
                return value;
            });
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException ignore) {
            }
        });
        kafkaOrderedProcessor.start();
        int expectedMessages = partitions * messagesPerPartition;
        long startTime = System.currentTimeMillis();
        while (processed.get() < expectedMessages) {
            //noinspection BusyWait
            Thread.sleep(1000);
            if (System.currentTimeMillis() - startTime > 30000) {
                Assertions.fail("Test taking more than expected time");
            }
        }
        Assertions.assertEquals(processed.get(), expectedMessages);
        Assertions.assertTrue(keyVsErrors.isEmpty());
        kafkaOrderedProcessor.shutdown(Duration.ofSeconds(10));
    }

    private void createTopic() {
        Admin admin = Admin.create(kafkaConfig.toProperties());
        admin.deleteTopics(Collections.singleton(kafkaConfig.getTopic()));
        admin.createTopics(Collections.singleton(new NewTopic(kafkaConfig.getTopic(), 50, (short) 1)));
        admin.close();
    }

    @SuppressWarnings("SameParameterValue")
    private void produceMessages(int partitions, int numberOfMessagesPerPartition) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.toProperties(), new StringSerializer(), new StringSerializer());
        for (int i = 0; i < partitions; i++) {
            final String key = "PartitionKey_" + i;
            for (int j = 0; j < numberOfMessagesPerPartition; j++) {
                producer.send(new ProducerRecord<>(kafkaConfig.getTopic(), key, key + ":" + j));
            }
        }
        producer.flush();
    }
}
