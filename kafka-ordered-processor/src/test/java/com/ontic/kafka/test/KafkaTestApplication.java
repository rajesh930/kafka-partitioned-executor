package com.ontic.kafka.test;

import com.ontic.kafka.KafkaConfig;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * @author rajesh
 * @since 17/02/25 12:19
 */
@SpringBootApplication
@EnableConfigurationProperties
public class KafkaTestApplication {

    @Bean
    @ConfigurationProperties(prefix = "ontic.kafka")
    public KafkaConfig<String, String> config() {
        return new KafkaConfig<>();
    }
}
