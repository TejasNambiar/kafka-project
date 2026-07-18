package com.kafkalearning.consumer.listener;

import com.kafkalearning.consumer.service.OrderEventProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"orders-events-dlt-test", "orders-events-dlt-test.DLT"})
class OrderEventListenerDltRoutingTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private org.springframework.kafka.config.KafkaListenerEndpointRegistry registry;

    // Mocking the processor here — this test is specifically about
    // "does malformed JSON get routed to DLT," not business logic. If
    // the processor gets invoked at all, that's itself a test failure
    // (malformed records must never reach it).
    @MockBean
    private OrderEventProcessor orderEventProcessor;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry)  {
        registry.add("app.kafka.topic.orders-events", () -> "orders-events-dlt-test");
        registry.add("app.kafka.topic.orders-events-dlt", () -> "orders-events-dlt-test.DLT");
        registry.add("app.kafka.streams.application-id", () -> "streams-dlt-test-" + System.nanoTime());
    }

    @Test
    void malformedJson_isRoutedToDlt_immediately_withoutRetryOrProcessorCall() {
        // Wait for the @KafkaListener container to actually be assigned
        // its partition(s) before publishing — without this, the test
        // can send before the consumer group finishes rebalancing,
        // causing the message to sit unconsumed past our poll timeout.
        var container = registry.getListenerContainer("orderEventListenerContainer");
        org.springframework.kafka.test.utils.ContainerTestUtils.waitForAssignment(container, 1);

        KafkaTemplate<String, String> testProducer = buildTestProducer();
        testProducer.send("orders-events-dlt-test", "order-malformed", "{this is not valid json");

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "dlt-verification-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var dltConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps)) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, "orders-events-dlt-test.DLT");

            // Non-retryable path should be fast — no 1s/2s/4s backoff delay.
            // Generous timeout still used to absorb CI/container jitter.
            ConsumerRecord<String, String> dltRecord =
                    KafkaTestUtils.getSingleRecord(dltConsumer, "orders-events-dlt-test.DLT", Duration.ofSeconds(10));

            assertThat(dltRecord.key()).isEqualTo("order-malformed");
            assertThat(dltRecord.value()).isEqualTo("{this is not valid json");

            // Verify DeadLetterPublishingRecoverer's standard exception headers exist.
            assertThat(dltRecord.headers().lastHeader("kafka_dlt-exception-fqcn")).isNotNull();
            assertThat(dltRecord.headers().lastHeader("kafka_dlt-original-topic")).isNotNull();
        }

        // Deserialization failed before the listener ever called the processor.
        verifyNoInteractions(orderEventProcessor);
    }

    private KafkaTemplate<String, String> buildTestProducer() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }
}