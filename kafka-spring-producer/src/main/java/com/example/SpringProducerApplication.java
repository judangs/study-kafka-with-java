package com.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

//@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void run(String... args) throws Exception {
        for(int i=0; i<10; i++) {
//            kafkaTemplate.send(TOPIC_NAME, "test" + i);
        }
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }
}
