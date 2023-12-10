package org.example.batchkafka;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class BatchKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchKafkaApplication.class, args);
	}

}
