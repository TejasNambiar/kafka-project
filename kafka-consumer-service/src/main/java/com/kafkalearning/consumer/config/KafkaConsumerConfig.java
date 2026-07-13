package com.kafkalearning.consumer.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Explicit ConsumerFactory/ListenerContainerFactory beans built from
 * Boot's auto-detected KafkaProperties. As with the producer side, this
 * is declared explicitly (rather than relying on implicit auto-config)
 * to give us a visible seam for Phase 3's error-handling configuration.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    /**
     * Routes every failed record to the same DLT topic/partition
     * mapping regardless of the original partition — simplest
     * strategy; per-partition DLT topics are possible but add
     * complexity we don't need at this scale.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, String> dltKafkaTemplate,
            @Value("${app.kafka.topic.orders-events-dlt}") String dltTopicName) {
        return new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, exception) ->
                        new TopicPartition(dltTopicName, record.partition()%3)
                );
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer){
        // 3 retry attempts: 1s, 2s, 4s delays (capped at 10s max interval).
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(15_000L); // ceiling on total retry duration

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Deserialization/format errors can never succeed on retry —
        // route straight to DLT without burning retry attempts.
        errorHandler.addNotRetryableExceptions(
                JsonProcessingException.class,
                SerializationException.class,
                IllegalArgumentException.class
        );

        return errorHandler;
    }
}
