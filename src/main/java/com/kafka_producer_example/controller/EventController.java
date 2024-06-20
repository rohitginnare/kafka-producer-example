package com.kafka_producer_example.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka_producer_example.dto.Customer;
import com.kafka_producer_example.service.KafkaMessagePublisher;

@RestController
@RequestMapping("/producer-app")
public class EventController {

	@Autowired
	private KafkaMessagePublisher kafkaMessagePublisher;

	@GetMapping("/publish/{message}")
	public ResponseEntity<?> publishMessage(@PathVariable String message) {
		try {

			for (int i = 0; i < 10000; i++) {
				kafkaMessagePublisher.sendMessageToTopic(message + " : " + i);
			}
			return ResponseEntity.ok("Message Pusblished successfully!");

		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}

	@PostMapping("/publish")
	public ResponseEntity<?> publishCustomer(@RequestBody Customer customer) {
		try {
			for (int i = 0; i < 2000; i++) {
				kafkaMessagePublisher.sendEventsToTopic(customer);
			}
			System.out.println("Message " + customer.toString() + "Published Succesfully...!");
			return ResponseEntity.ok("Message " + customer.toString() + "Published Succesfully...!");

		} catch (Exception e) {
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping("/publishOnSinglePartition/{message}")
	public ResponseEntity<?> getMessage(@PathVariable String message) {
		try {
			for (int i = 1; i <= 100; i++) {
				kafkaMessagePublisher.publishMessageOnSinglePartition(message);
			}

			System.out.println("Message Published on Single topic successfully. Message : " + message);
			return new ResponseEntity<>(HttpStatus.OK);
		} catch (Exception e) {
			System.out.println("Unable to send message to the topic.");
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@GetMapping("/publishMessageforRetry/{message}")
	public ResponseEntity<?> getMessagesForRetry(@PathVariable String message) {
		try {
			for (int i = 1; i <= 88; i++) {
				kafkaMessagePublisher.publishMessageforRetry(message);
			}
			
			System.out.println("Message Published on Single topic successfully. Message : " + message);
			return new ResponseEntity<>(HttpStatus.OK);
		} catch (Exception e) {
			System.out.println("Unable to send message to the topic.");
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
}