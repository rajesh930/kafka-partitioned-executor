package com.ontic.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Properties;

/**
 * @author rajesh
 * @since 13/02/25 19:01
 */
public class KafkaConfig<K, V> {
    private String topic;
    private String bootstrapServers;
    private String groupId;
    private String groupInstanceId;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private String offsetResetConfig = "earliest";
    private Integer maxMessagesPerPoll = 100;
    private Duration sessionTimeout;
    private Boolean enableAutoCommit;
    private Properties otherKafkaProperties;

    private int maxQueuedMessages = 1000;
    private Duration pollTimeout;
    private Duration maxMessageProcessingTime = Duration.ofMinutes(5);

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupInstanceId() {
        return groupInstanceId;
    }

    public void setGroupInstanceId(String groupInstanceId) {
        this.groupInstanceId = groupInstanceId;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(Deserializer<K> keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(Deserializer<V> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    public String getOffsetResetConfig() {
        return offsetResetConfig;
    }

    public void setOffsetResetConfig(String offsetResetConfig) {
        this.offsetResetConfig = offsetResetConfig;
    }

    public Integer getMaxMessagesPerPoll() {
        return maxMessagesPerPoll;
    }

    public void setMaxMessagesPerPoll(Integer maxMessagesPerPoll) {
        this.maxMessagesPerPoll = maxMessagesPerPoll;
    }

    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public Boolean getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(Boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public Properties getOtherKafkaProperties() {
        return otherKafkaProperties;
    }

    public void setOtherKafkaProperties(Properties otherKafkaProperties) {
        this.otherKafkaProperties = otherKafkaProperties;
    }

    public int getMaxQueuedMessages() {
        return maxQueuedMessages;
    }

    public void setMaxQueuedMessages(int maxQueuedMessages) {
        this.maxQueuedMessages = maxQueuedMessages;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public Duration getMaxMessageProcessingTime() {
        return maxMessageProcessingTime;
    }

    public void setMaxMessageProcessingTime(Duration maxMessageProcessingTime) {
        this.maxMessageProcessingTime = maxMessageProcessingTime;
    }

    public Properties toProperties() {
        Properties props;
        if (otherKafkaProperties != null) {
            props = new Properties(otherKafkaProperties);
        } else {
            props = new Properties();
        }
        if (bootstrapServers != null) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        if (groupId != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        if (groupInstanceId != null) {
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        }
        if (keyDeserializer != null) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        }
        if (valueDeserializer != null) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        }
        if (offsetResetConfig != null) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);
        }
        if (maxMessagesPerPoll != null) {
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessagesPerPoll);
        }
        if (enableAutoCommit != null) {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString());
        }
        if (sessionTimeout != null) {
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeout.toMillis()));
        }
        return props;
    }
}
