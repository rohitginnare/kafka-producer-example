package com.kafka_producer_example.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.kafka_producer_example.dto.Customer;

@Service
public class KafkaMessagePublisher {

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	public void sendMessageToTopic(String message) {

		try {

			CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("javatechie-customer", message);
			future.whenComplete((result, exception) -> {
				if (exception == null) {
					System.out.println("Sent message : " + message + " with offset : "
							+ result.getRecordMetadata().offset() + "]");
				} else {
					System.out.println("Unable to send message beacuse of Exception : " + exception.getMessage());
				}

			});
		} catch (Exception e) {
			System.out.println("Error while Publishing message..." + e.getMessage());
		}
	}

	public void sendEventsToTopic(Customer customer) {

		try {

			CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("javatechie-customer", customer);
			future.whenComplete((result, exception) -> {
				if (exception == null) {
					System.out.println("Sent message : " + customer.toString() + " with offset : "
							+ result.getRecordMetadata().offset() + "]");
				} else {
					System.out.println("Unable to send message beacuse of Exception : " + exception.getMessage());
				}

			});
		} catch (Exception e) {
			System.out.println("Error while Publishing message..." + e.getMessage());
		}
	}
}
