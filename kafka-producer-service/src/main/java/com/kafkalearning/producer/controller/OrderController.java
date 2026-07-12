package com.kafkalearning.producer.controller;

import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkalearning.producer.event.OrderEvent;
import com.kafkalearning.producer.service.OrderEventProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    public record CreateOrderRequest(String customerId, double amount) {
    }

    @PostMapping("/createOrder")
    public ResponseEntity<OrderEvent> createOrder(@RequestBody CreateOrderRequest request) {
        log.info("calling createOrder: " + request);
        OrderEvent event = OrderEvent.created(
                UUID.randomUUID().toString(),
                request.customerId(),
                request.amount());
        orderEventProducer.publishOrderEvent(event);
        return ResponseEntity.ok().body(event);
    }
}