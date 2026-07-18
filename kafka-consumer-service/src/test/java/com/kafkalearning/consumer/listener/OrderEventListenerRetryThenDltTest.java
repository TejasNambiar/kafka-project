package com.kafkalearning.consumer.listener;

import com.kafkalearning.consumer.event.OrderEvent;
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
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"orders-events-retry-test", "orders-events-retry-test.DLT"})
class OrderEventListenerRetryThenDltTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    // Real processor bean is replaced here — this test controls exactly
    // when it fails, simulating a transient downstream outage that
    // resolves after retries are exhausted (proving DLT is the correct
    // terminal outcome, not a successful eventual retry).
    @MockBean
    private OrderEventProcessor orderEventProcessor;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.topic.orders-events", () -> "orders-events-retry-test");
        registry.add("app.kafka.topic.orders-events-dlt", () -> "orders-events-retry-test.DLT");
        registry.add("app.kafka.streams.application-id", () -> "streams-retry-test-" + System.nanoTime());
    }

//    @Test
    void persistentFailure_retries3Times_thenRoutesToDlt() throws Exception {
        AtomicInteger invocationCount = new AtomicInteger(0);

        // Always throws — this is the "permanently down downstream
        // dependency" scenario, not a "succeeds on 2nd try" scenario.
        // We're proving retries happen AND that exhaustion correctly
        // falls through to DLT, not that retry-then-succeed works
        // (that path is simpler and lower-risk — this is the harder case).
        doAnswer(invocation -> {
            invocationCount.incrementAndGet();
            throw new RuntimeException("simulated persistent downstream failure");
        }).when(orderEventProcessor).process(any(OrderEvent.class));

        KafkaTemplate<String, String> testProducer = buildTestProducer();
        OrderEvent event = new OrderEvent("order-retry-1", "cust-retry", "CREATED", 15.0, Instant.now());
        String json = new com.fasterxml.jackson.databind.ObjectMapper()
                .findAndRegisterModules().writeValueAsString(event);

        testProducer.send("orders-events-retry-test", "order-retry-1", json);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "retry-verification-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var dltConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps)) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, "orders-events-retry-test.DLT");

            // Backoff is 1s, 2s, 4s (capped, maxElapsedTime 15s) — so the
            // DLT record won't appear instantly. Generous timeout here
            // absorbs that real elapsed time plus CI jitter.
            ConsumerRecord<String, String> dltRecord =
                    KafkaTestUtils.getSingleRecord(dltConsumer, "orders-events-retry-test.DLT", Duration.ofSeconds(20));

            assertThat(dltRecord.key()).isEqualTo("order-retry-1");
        }

        // 1 initial attempt + 3 retries = 4 total invocations.
        verify(orderEventProcessor, times(4)).process(any(OrderEvent.class));
        assertThat(invocationCount.get()).isEqualTo(4);
    }

    private KafkaTemplate<String, String> buildTestProducer() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }
}