package com.example.batch;

import com.example.record.SpringConsumerApplication;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;


/*
    spring:
      kafka:
        consumer:
          bootstrap-servers: localhost:9092
        listener:
          type: batch

     카프카 컨슈머 설정이 위와 같을 경우 BatchMessageListener를 구현해 메시지를 받아오게 된다.
 */
@SpringBootApplication
public class BatchMessage {

    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);


    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(BatchMessage.class);
        application.run(args);
    }

    /*
        배치 리스너는 레코드 리스너와 다르게 메서드의 파라미터를 List 또는 ConsumerRecords로 받는다.
     */
    @KafkaListener(topics = "test",
            groupId = "test-group-01")
    public void batchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> {
            logger.info(record.toString());
        });
    }

    @KafkaListener(topics = "test",
            groupId = "test-group-02")
    public void batchListener(List<String> records) {
        records.forEach(record -> logger.info(record));
    }

    @KafkaListener(topics = "test",
            groupId = "test-group-03",
            concurrency = "3")
    public void concurrentBatchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> {logger.info(record.toString());});
    }

}
