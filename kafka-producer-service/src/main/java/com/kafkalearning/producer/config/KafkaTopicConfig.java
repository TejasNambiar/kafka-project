package com.kafkalearning.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Explicit topic provisioning for {@code orders-events}.
 *
 * <p>Added in Phase 3, replacing reliance on
 * {@code auto.create.topics.enable=true} (used through Phase 1-2). Once
 * a second topic (the DLT) entered the picture with real partition-count
 * expectations tied to this one, implicit auto-creation stopped being
 * safe — auto-created topics get default partition/replication settings
 * that are rarely what's actually wanted, and a typo in a topic name
 * would silently create a garbage topic instead of failing loudly.
 *
 * <p>Spring's {@code KafkaAdmin} auto-detects any {@link NewTopic} bean
 * in the context and creates it on startup, idempotently.
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
