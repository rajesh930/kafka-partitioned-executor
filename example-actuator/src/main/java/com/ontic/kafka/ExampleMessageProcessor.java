package com.ontic.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Duration;

/**
 * @author rajesh
 * @since 17/02/25 21:38
 */
@SpringBootApplication
public class ExampleMessageProcessor implements CommandLineRunner, DisposableBean {
    private KafkaOrderedProcessor<String, String> processor;

    @Bean
    @ConfigurationProperties(prefix = "ontic.kafka")
    public KafkaConfig<String, String> config() {
        return new KafkaConfig<>();
    }

    public static void main(String[] args) {
        SpringApplication.run(ExampleMessageProcessor.class, args);
    }

    @Override
    public void run(String... args) {
        KafkaConfig<String, String> kafkaConfig = config();
        kafkaConfig.setKeyDeserializer(new StringDeserializer());
        kafkaConfig.setValueDeserializer(new StringDeserializer());
        this.processor = new KafkaOrderedProcessor<>(kafkaConfig, s -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println(s);
        });
        processor.start();
    }

    @Override
    public void destroy() throws Exception {
        this.processor.shutdown(Duration.ofSeconds(60));
    }
}