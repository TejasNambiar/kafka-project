package com.kafkalearning.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Explicit topic provisioning. Spring's KafkaAdmin auto-detects NewTopic
 * beans and creates them on application startup (idempotently — no error
 * if the topic already exists), independent of the broker's
 * auto.create.topics.enable setting.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersEventsTopic(@Value("${app.kafka.topic.orders-events}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1) // single-broker local dev; never 1 in production
                .build();

    }
}
