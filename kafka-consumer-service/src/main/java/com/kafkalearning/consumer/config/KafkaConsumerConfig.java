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
 * Consumer/listener-container wiring, including Phase 3's retry/backoff
 * and dead-letter-topic configuration.
 *
 * <p><b>Historical bug, worth knowing about:</b> {@link #errorHandler}
 * and {@link #kafkaListenerContainerFactory} were originally two
 * independent beans that were never actually connected —
 * {@code setCommonErrorHandler()} was missing from the factory method.
 * The error handler bean existed, was fully configured, and was simply
 * never attached to anything. Spring did not error; it silently fell
 * back to its own internal default handler with no DLT recoverer. This
 * is why {@link #kafkaListenerContainerFactory} explicitly takes
 * {@link org.springframework.kafka.listener.DefaultErrorHandler} as a
 * parameter and calls {@code setCommonErrorHandler} — that wiring is the
 * fix, and its absence is the kind of bug that compiles cleanly and
 * looks correct in review.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
    }

    /**
     * Explicitly wires {@code errorHandler} into the container factory —
     * see the class-level doc above for why this line specifically is
     * the fix for a real bug where retry/DLT config was defined but
     * silently inert.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Routes every failed record to the DLT topic, mapping the original
     * partition to a DLT partition via {@code partition % 3} — a simple,
     * deterministic strategy that preserves some partition locality
     * without needing a separate DLT topic per source partition.
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

    /**
     * 3 retry attempts (1s, 2s, 4s, capped at 10s, 15s max elapsed) for
     * transient failures. {@link JsonProcessingException},
     * {@link SerializationException}, and {@link IllegalArgumentException}
     * are classified non-retryable — deserialization/format errors can
     * never succeed on retry, so these skip straight to the DLT instead
     * of burning all 3 attempts pointlessly.
     */
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
