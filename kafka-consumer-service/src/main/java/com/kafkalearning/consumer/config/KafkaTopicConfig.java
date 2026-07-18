package com.kafkalearning.consumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The DLT is owned by the consumer service, not the producer — it's the
 * consumer's failure-handling concern, not a contract the producer
 * publishes against.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersEventsDltTopic(@Value("${app.kafka.topic.orders-events-dlt}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(3) // matches source topic partition count
                .replicas(1) // single-broker local dev; never 1 in production
                .build();

    }
}
