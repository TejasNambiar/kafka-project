package com.kafkalearning.producer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.producer.event.OrderEvent;
import com.kafkalearning.producer.service.OrderEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderEventProducer orderEventProducer;

    @Test
    void createOrder_returns202_withGeneratedOrderEvent() throws Exception {
        OrderController.CreateOrderRequest request =
                new OrderController.CreateOrderRequest("cust-1", 49.99);

        mockMvc.perform(post("/api/orders/createOrder")
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").isNotEmpty())
                .andExpect(jsonPath("$.customerId").value("cust-1"))
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.amount").value(49.99));

        verify(orderEventProducer, times(1)).publishOrderEvent(any(OrderEvent.class));
    }

    @Test
    void createOrder_alwaysGeneratesUniqueOrderId_acrossRequests() throws Exception {
        OrderController.CreateOrderRequest request =
                new OrderController.CreateOrderRequest("cust-2", 10.0);
        String body = objectMapper.writeValueAsString(request);

        String firstResponse = mockMvc.perform(post("/api/orders/createOrder")
                        .contentType("application/json").content(body))
                .andReturn().getResponse().getContentAsString();
        String secondResponse = mockMvc.perform(post("/api/orders/createOrder")
                        .contentType("application/json").content(body))
                .andReturn().getResponse().getContentAsString();

        OrderEvent first = objectMapper.readValue(firstResponse, OrderEvent.class);
        OrderEvent second = objectMapper.readValue(secondResponse, OrderEvent.class);

        org.assertj.core.api.Assertions.assertThat(first.orderId()).isNotEqualTo(second.orderId());
    }
}